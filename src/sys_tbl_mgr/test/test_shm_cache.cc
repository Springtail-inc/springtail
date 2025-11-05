#include <gtest/gtest.h>
#include <common/common.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <unistd.h>
#include <sys/wait.h>
#include <memory>
#include <sstream>

using namespace springtail;

constexpr char CACHE_NAME[]="springtail.roots";
constexpr size_t CACHE_SIZE=1024*100;

TEST(ShmTest, Basic) {

    sys_tbl_mgr::ShmCache::remove(CACHE_NAME);

    sys_tbl_mgr::ShmCache c{CACHE_NAME, CACHE_SIZE};

    for (uint64_t i = 0; i != 100; ++i) {
        std::ostringstream os;
        os << i;
        auto b = c.insert(10000, 20000, 100+i, os.str());
        ASSERT_TRUE(b);
    }

    for (uint64_t i = 0; i != 100; ++i) {
        std::ostringstream os;
        os << i << "." << i;
        auto b = c.insert(i, i+1, 100+i, os.str());
        ASSERT_TRUE(b);
    }

    for (uint64_t i = 0; i != 100; ++i) {
        std::ostringstream os;
        os << i;
        auto r = c.find(10000, 20000, 100+i);
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(os.str(), r.value());
    }

    for (uint64_t i = 0; i != 100; ++i) {
        std::ostringstream os;
        os << i << "." << i;
        auto r = c.find(i, i+1, 100+i);
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(os.str(), r.value());
    }

    if (fork() == 0) {

        sys_tbl_mgr::ShmCache child_cache{CACHE_NAME};

        for (uint64_t i = 0; i != 100; ++i) {
            std::ostringstream os;
            os << i;
            auto r = child_cache.find(10000, 20000, 100+i);
            ASSERT_TRUE(r.has_value());
            ASSERT_EQ(os.str(), r.value());
        }

        for (uint64_t i = 0; i != 100; ++i) {
            std::ostringstream os;
            os << i << "." << i;
            auto r = child_cache.find(i, i+1, 100+i);
            ASSERT_TRUE(r.has_value());
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

TEST(ShmTest, Lifecycle) {
    sys_tbl_mgr::ShmCache::remove(CACHE_NAME);

    // try to open a cache that hasn't been created, it must throw.
    try {
        auto c = sys_tbl_mgr::ShmCache(CACHE_NAME); 
        // must not come here
        ASSERT_TRUE(false);
    } catch (const boost::interprocess::interprocess_exception&) {
    }

    // create the cache
    std::unique_ptr<sys_tbl_mgr::ShmCache> parent;
    parent = std::make_unique<sys_tbl_mgr::ShmCache>(CACHE_NAME, CACHE_SIZE); 

    // try to create another instance, it must throw.
    try {
        auto c = sys_tbl_mgr::ShmCache(CACHE_NAME, CACHE_SIZE); 
        // must not come here
        ASSERT_TRUE(false);
    } catch (const boost::interprocess::interprocess_exception&) {
    }

    //add some data to the parent
    for (uint64_t i = 0; i != 100; ++i) {
        std::ostringstream os;
        os << i;
        auto b = parent->insert(10000, 20000, 100+i, os.str());
        ASSERT_TRUE(b);
    }

    // open the cache
    {
        auto c = sys_tbl_mgr::ShmCache(CACHE_NAME); 
        // read it
        for (uint64_t i = 0; i != 100; ++i) {
            std::ostringstream os;
            os << i;
            auto r = c.find(10000, 20000, 100+i);
            ASSERT_TRUE(r.has_value());
            ASSERT_EQ(os.str(), r.value());
        }

        auto db_tables = c.get_db_tables(10000);
        ASSERT_EQ(db_tables.size(), 1);

        // drop the table at some future xid
        c.mark_dropped(10000, 20000, 500);

        db_tables = c.get_db_tables(10000);
        // no tables now
        ASSERT_EQ(db_tables.size(), 0);

        // include dropped tables
        db_tables = c.get_db_tables(10000, false);
        ASSERT_EQ(db_tables.size(), 1);

        // but should still be able to access previous xid's
        auto r = c.find(10000, 20000, 105);
        ASSERT_TRUE(r.has_value());

        // kill the parent
        parent.reset();

        // create new parent
        parent = std::make_unique<sys_tbl_mgr::ShmCache>(CACHE_NAME, CACHE_SIZE); 
        
        // should still have the old read
        for (uint64_t i = 0; i != 100; ++i) {
            std::ostringstream os;
            os << i;
            auto r = c.find(10000, 20000, 100+i);
            ASSERT_TRUE(r.has_value());
            ASSERT_EQ(os.str(), r.value());
        }

        // the new parent should be empty
        for (uint64_t i = 0; i != 100; ++i) {
            std::ostringstream os;
            os << i;
            auto r = parent->find(10000, 20000, 100+i);
            ASSERT_FALSE(r.has_value());
        }
    }
}

TEST(ShmTest, BasicEviction) {

    sys_tbl_mgr::ShmCache::remove(CACHE_NAME);

    sys_tbl_mgr::ShmCache c{CACHE_NAME, CACHE_SIZE};

    for (uint64_t i = 0; i != 10000; ++i) {
        std::ostringstream os;
        os << i << "." << i;
        auto b = c.insert(i, i+1, 100+i, os.str());
        ASSERT_TRUE(b);
        if (i) {
            //keep accessing the first element
            auto r = c.find(0, 1, 100);
            ASSERT_TRUE(r.has_value());
        }
    }

    ASSERT_GT(c.size(), 100);
    ASSERT_LT(c.size(), 10000);

    // the first element must be present
    auto r = c.find(0, 1, 100);
    ASSERT_TRUE(r.has_value());

    // a few first elements must be evicted
    for (uint64_t i = 1; i != 10; ++i) {
        std::ostringstream os;
        os << i << "." << i;
        auto r = c.find(i, i+1, 100+i);
        ASSERT_FALSE(r.has_value());
    }

    // check the last 100 elements
    for (uint64_t i = 10000-1; i != 9900; --i) {
        std::ostringstream os;
        os << i << "." << i;
        auto r = c.find(i, i+1, 100+i);
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(os.str(), r.value());
    }

    // do the same for the same db/tid
    for (uint64_t i = 0; i != 10000; ++i) {
        std::ostringstream os;
        os << i;
        auto b = c.insert(10000, 20000, 100+i, os.str());
        ASSERT_TRUE(b);
        if (i) {
            //keep accessing the first element
            auto r = c.find(10000, 20000, 100);
            ASSERT_TRUE(r.has_value());
        }
    }

    ASSERT_GT(c.size(), 100);
    ASSERT_LT(c.size(), 10000);

    // check the last 100 elements
    for (uint64_t i = 10000-1; i != 9900; --i) {
        std::ostringstream os;
        os << i;
        auto r = c.find(10000, 20000, 100+i);
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(os.str(), r.value());
    }

    // the first element must be present
    r = c.find(10000, 20000, 100);
    ASSERT_TRUE(r.has_value());
}

TEST(ShmTest, XidUpdates) {
    sys_tbl_mgr::ShmCache::remove(CACHE_NAME);
    sys_tbl_mgr::ShmCache c{CACHE_NAME, CACHE_SIZE};

    uint64_t db = 1;
    uint64_t xid = 100;

    // Initially, get_committed_xid should return nullopt
    auto result = c.get_committed_xid(db, 0);
    ASSERT_FALSE(result.has_value());

    // Update committed XID without schema changes
    c.update_committed_xid(db, xid, true);

    // Now should get the XID back
    result = c.get_committed_xid(db, xid);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), xid);

    // Update to a higher XID
    uint64_t xid2 = 200;
    c.update_committed_xid(db, xid2, false);

    result = c.get_committed_xid(db, xid);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), xid2);

    // Update to a higher XID
    uint64_t xid3 = 300;
    c.update_committed_xid(db, xid3, true);

    result = c.get_committed_xid(db, xid);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), xid2);

    result = c.get_committed_xid(db, xid3);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), xid3);
}
