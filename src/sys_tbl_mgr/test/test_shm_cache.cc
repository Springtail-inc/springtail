#include <gtest/gtest.h>
#include <common/common.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <unistd.h>
#include <sys/wait.h>
#include <sstream>

using namespace springtail;


TEST(ShmTest, Basic) {
    constexpr char CACHE_NAME[]="springtail.roots";
    constexpr size_t CACHE_SIZE=1024*10;

    sys_tbl_mgr::ShmCache::remove(CACHE_NAME);

    sys_tbl_mgr::ShmCache c{CACHE_NAME, CACHE_SIZE};

    for (uint64_t i = 0; i != 100; ++i) {
        std::ostringstream os;
        os << i;
        c.insert(10000, 20000, 100+i, os.str());
    }

    for (uint64_t i = 0; i != 100; ++i) {
        std::ostringstream os;
        os << i << "." << i;
        c.insert(i, i+1, 100+i, os.str());
    }

    if (fork() == 0) {

        sys_tbl_mgr::ShmCache child_cache{CACHE_NAME, CACHE_SIZE};

        for (uint64_t i = 0; i != 100; ++i) {
            std::ostringstream os;
            os << i;
            auto r = child_cache.find(10000, 20000, 100+i);
            ASSERT_EQ(os.str(), r.value());
        }

        for (uint64_t i = 0; i != 100; ++i) {
            std::ostringstream os;
            os << i << "." << i;
            auto r = child_cache.find(i, i+1, 100+i);
            ASSERT_EQ(os.str(), r.value());
        }

        exit(0);
    } else {
        int status;
        wait(&status);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
    }
}

