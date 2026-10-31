#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <storage/index_engine/shared_extent_pool.hh>

using namespace springtail;

namespace ipc = boost::interprocess;

namespace {
constexpr char POOL_NAME[] = "springtail.test.extent_pool";
constexpr uint64_t POOL_SIZE = 4 * 1024 * 1024;

// Small pools used by the eviction/pin tests, sized to hold only a handful of the tiny
// extents those tests publish (each well under 1KB serialized).
constexpr uint64_t SMALL_POOL_SIZE = 64 * 1024;

/** Builds a minimal, valid, single-row Extent -- no schema/Field machinery needed, since
    SharedExtentPool is content-agnostic (it only moves ExtentHeader::pack()/
    Extent::serialize() bytes around). */
std::shared_ptr<Extent>
make_extent(uint64_t xid, uint64_t value)
{
    auto extent = std::make_shared<Extent>(ExtentType(/*branch=*/false), xid, sizeof(uint64_t),
                                           std::vector<uint8_t>{});
    auto row = extent->append();
    std::memcpy(row.data(), &value, sizeof(value));
    return extent;
}

uint64_t
extent_value(Extent &extent)
{
    uint64_t value;
    std::memcpy(&value, extent.begin()->data(), sizeof(value));
    return value;
}

/** Removes `name`'s backing shared memory object at scope exit, regardless of how the test
    function returns (including an early ASSERT_* return) -- avoids leaking POSIX shared
    memory objects across test runs/processes on failure. */
struct ShmObjectGuard {
    std::string name;
    ~ShmObjectGuard() { ipc::shared_memory_object::remove(name.c_str()); }
};

class SharedExtentPoolTest : public testing::Test {
protected:
    // Deliberately no springtail_init_test()/shutdown() here, mirroring
    // src/sys_tbl_mgr/test/test_shm_cache.cc's ShmTest fixture -- these tests fork(), and
    // SharedExtentPool's explicit (name, size) constructor never touches Properties, so
    // there is nothing for a full init to provide that these tests need.
    void SetUp() override { SharedExtentPool::remove(POOL_NAME); }
    void TearDown() override { SharedExtentPool::remove(POOL_NAME); }
};

TEST_F(SharedExtentPoolTest, PublishOnceReuseWithoutRefetch)
{
    SharedExtentPool pool(POOL_NAME, POOL_SIZE);
    SharedExtentPool::Key key{"file_a", 1};

    int fetch_calls = 0;
    auto fetch = [&]() {
        ++fetch_calls;
        return make_extent(5, 111);
    };

    auto e1 = pool.get(key, fetch);
    ASSERT_EQ(extent_value(*e1), 111u);
    ASSERT_EQ(fetch_calls, 1);
    ASSERT_TRUE(pool.contains(key));

    // second get() for the same key must not call fetch() again.
    auto e2 = pool.get(key, fetch);
    ASSERT_EQ(extent_value(*e2), 111u);
    ASSERT_EQ(fetch_calls, 1);
}

TEST_F(SharedExtentPoolTest, DistinctKeysDoNotCollide)
{
    SharedExtentPool pool(POOL_NAME, POOL_SIZE);

    SharedExtentPool::Key key_a{"file_a", 1};
    SharedExtentPool::Key key_b{"file_b", 1};  // same extent_id, different file
    SharedExtentPool::Key key_c{"file_a", 2};  // same file, different extent_id

    auto a = pool.get(key_a, [] { return make_extent(1, 10); });
    auto b = pool.get(key_b, [] { return make_extent(1, 20); });
    auto c = pool.get(key_c, [] { return make_extent(1, 30); });

    ASSERT_EQ(extent_value(*a), 10u);
    ASSERT_EQ(extent_value(*b), 20u);
    ASSERT_EQ(extent_value(*c), 30u);
}

TEST_F(SharedExtentPoolTest, LeastRecentlyUsedEvictedFirst)
{
    SharedExtentPool pool(POOL_NAME, SMALL_POOL_SIZE);

    // insert enough distinct keys to force eviction well before reaching this count.
    for (uint64_t i = 0; i < 500; ++i) {
        SharedExtentPool::Key key{"file_evict", i};
        pool.get(key, [i] { return make_extent(1, i); });
    }

    ASSERT_LT(pool.entry_count(), 500u);

    // the earliest-inserted, never-revisited keys should have been evicted...
    ASSERT_FALSE(pool.contains(SharedExtentPool::Key{"file_evict", 0}));
    // ...while the most recently published key must still be resident.
    ASSERT_TRUE(pool.contains(SharedExtentPool::Key{"file_evict", 499}));
}

TEST_F(SharedExtentPoolTest, PinProtectsEntryFromEvictionPressure)
{
    SharedExtentPool pool(POOL_NAME, SMALL_POOL_SIZE);

    SharedExtentPool::Key pinned_key{"file_pin", 0};
    pool.get(pinned_key, [] { return make_extent(1, 0); });

    auto pin = pool.try_pin(pinned_key);
    ASSERT_TRUE(pin.has_value());

    // flood the pool with other entries -- enough to exceed SMALL_POOL_SIZE many times over
    // -- so an eviction pass actually targets `pinned_key` (the least-recently-touched entry)
    // and not just "some entry or other".
    for (uint64_t i = 1; i < 500; ++i) {
        SharedExtentPool::Key key{"file_pin", i};
        pool.get(key, [i] { return make_extent(1, i); });
    }

    ASSERT_TRUE(pool.contains(pinned_key)) << "pinned entry was evicted under pressure";

    pin.reset();  // release the pin

    // repeat the flood -- now that it's unpinned, the (still least-recently-used) entry
    // must become evictable again.
    for (uint64_t i = 500; i < 1000; ++i) {
        SharedExtentPool::Key key{"file_pin", i};
        pool.get(key, [i] { return make_extent(1, i); });
    }

    ASSERT_FALSE(pool.contains(pinned_key)) << "entry was never evicted once unpinned";
}

/**
 * Multi-process regression test for OQ6 (see shared_extent_pool.hh): spawns several
 * processes that all race to be the first to fetch the *same* key, after synchronizing on a
 * barrier to make the race as tight as possible, and asserts the underlying "decompression"
 * (the `fetch` callback) ran exactly once across every one of them -- not "usually once".
 * Mirrors src/sys_tbl_mgr/test/test_shm_cache.cc's ShmTest.Basic fork()+waitpid() pattern.
 *
 * This was verified to actually discriminate (not just pass by luck) during development by
 * temporarily removing the in-flight gate in SharedExtentPool::get() and re-running: with the
 * gate removed, every child raced straight to fetch() and the counter below reliably read
 * NUM_CHILDREN instead of 1, confirming this test would catch a regression in the gate rather
 * than passing regardless (phase 1/3/4's "assert on a signal that actually discriminates"
 * lesson, applied here).
 */
TEST_F(SharedExtentPoolTest, ExactlyOneFetchAcrossRacingProcesses)
{
    constexpr char COUNTER_SHM[] = "springtail.test.extent_pool.counters";
    constexpr int NUM_CHILDREN = 6;

    struct Counters {
        std::atomic<int> ready{0};
        std::atomic<int> go{0};
        std::atomic<int> fetch_count{0};
    };

    ShmObjectGuard guard{COUNTER_SHM};
    ipc::shared_memory_object::remove(COUNTER_SHM);
    ipc::shared_memory_object shm(ipc::create_only, COUNTER_SHM, ipc::read_write);
    shm.truncate(sizeof(Counters));
    ipc::mapped_region region(shm, ipc::read_write);
    auto *counters = new (region.get_address()) Counters();

    SharedExtentPool::Key key{"file_race", 42};

    std::vector<pid_t> pids;
    for (int i = 0; i < NUM_CHILDREN; ++i) {
        pid_t pid = fork();
        ASSERT_NE(pid, -1);
        if (pid == 0) {
            // CHILD: attach to the same named pool, wait for the barrier, then race.
            SharedExtentPool pool(POOL_NAME, POOL_SIZE);

            counters->ready.fetch_add(1);
            while (counters->go.load() == 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }

            auto fetch = [counters]() {
                counters->fetch_count.fetch_add(1);
                // widen the race window so every child is genuinely contending, not just
                // the first one to reach the mutex.
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return make_extent(7, 999);
            };

            auto extent = pool.get(key, fetch);
            _exit(extent_value(*extent) == 999 ? 0 : 1);
        }
        pids.push_back(pid);
    }

    // release the barrier once every child has attached and is waiting on it.
    while (counters->ready.load() < NUM_CHILDREN) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    counters->go.store(1);

    for (auto pid : pids) {
        int status = 0;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        ASSERT_TRUE(WIFEXITED(status)) << "child " << pid << " did not exit normally";
        ASSERT_EQ(WEXITSTATUS(status), 0) << "child " << pid << " got the wrong extent content";
    }

    ASSERT_EQ(counters->fetch_count.load(), 1)
        << "expected exactly one decompression across all racing processes";
}

/**
 * Round-1 review Finding 2: two same-process threads racing on a never-before-seen key must
 * observe the *same* `Extent` object -- the same-object-identity invariant
 * `IndexReader::Iterator::Frame` depends on (see shared_extent_pool.hh's "Identity invariant"
 * section). Verified to actually discriminate: with `SharedExtentPool::get()`'s process-local
 * single-flight gate removed (routing straight into `_get_and_publish()`), this reliably
 * produced `NUM_THREADS` distinct `Extent` identities and a `fetch_count` of `NUM_THREADS`
 * instead of 1, reproduced across repeated runs; restoring the gate made it pass again.
 */
TEST_F(SharedExtentPoolTest, ConcurrentSameProcessCallsReturnSameIdentity)
{
    SharedExtentPool pool(POOL_NAME, POOL_SIZE);
    SharedExtentPool::Key key{"file_concurrent", 0};

    constexpr int NUM_THREADS = 8;
    std::atomic<int> fetch_count{0};
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};

    auto fetch = [&fetch_count]() {
        fetch_count.fetch_add(1);
        // widen the race window so every thread is genuinely contending, not just the first
        // one to reach the lock.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return make_extent(1, 777);
    };

    std::vector<std::shared_ptr<Extent>> results(NUM_THREADS);
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&, i]() {
            ready.fetch_add(1);
            while (!go.load()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            results[i] = pool.get(key, fetch);
        });
    }

    while (ready.load() < NUM_THREADS) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    go.store(true);

    for (auto &t : threads) {
        t.join();
    }

    ASSERT_EQ(fetch_count.load(), 1)
        << "expected exactly one decompression across all racing threads";
    for (int i = 0; i < NUM_THREADS; ++i) {
        ASSERT_EQ(extent_value(*results[i]), 777u);
        ASSERT_EQ(results[i].get(), results[0].get())
            << "thread " << i << " got a distinct Extent object instead of sharing thread 0's";
    }
}

/**
 * Round-1 review Finding 3: a pin must be released even if an exception is thrown while it's
 * held (e.g. `SharedExtentPool::_get_and_publish()`'s hit path throwing from `unpack_extent()`
 * after pinning but before storing the result) -- not just on the normal-return path, or a
 * pinned entry would permanently resist eviction (a one-slot leak per occurrence).
 */
TEST_F(SharedExtentPoolTest, PinReleasedWhenExceptionThrownWhileHeld)
{
    SharedExtentPool pool(POOL_NAME, SMALL_POOL_SIZE);
    SharedExtentPool::Key key{"file_pin_exception", 0};
    pool.get(key, [] { return make_extent(1, 0); });

    try {
        auto pin = pool.try_pin(key);
        ASSERT_TRUE(pin.has_value());
        throw std::runtime_error("simulated failure while pin is held");
    } catch (const std::runtime_error &) {
        // expected -- `pin`'s destructor should have released it during unwinding.
    }

    // flood the pool so an eviction pass targets this (now least-recently-used) entry; if the
    // pin above leaked, pin_count would still be > 0 and evict_for_space_locked() would skip
    // it forever.
    for (uint64_t i = 1; i < 500; ++i) {
        SharedExtentPool::Key flood_key{"file_pin_exception", i};
        pool.get(flood_key, [i] { return make_extent(1, i); });
    }
    ASSERT_FALSE(pool.contains(key)) << "pin leaked across an exception while held";
}
}  // namespace
