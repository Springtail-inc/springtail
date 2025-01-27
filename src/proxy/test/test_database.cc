#include <gtest/gtest.h>

#include <memory>

#include <common/common.hh>
#include <proxy/database.hh>
#include <proxy/server_session.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

namespace {

/** Mock server session */
class TestServerSession : public ServerSession {
public:
    TestServerSession(Session::Type type,
                      uint64_t id,
                      uint64_t db_id,
                      const std::string &database,
                      const std::string &username)
        : ServerSession(type, id, db_id, database, username)
    {
    }

    /** override for testing, connection is always active */
    bool is_connection_closed() const override { return false; }

    /** helper to set the db instance */
    void set_instance(DatabaseInstancePtr instance)
    {
        assert(instance != nullptr);
        _instance = instance;
    }
};
using TestServerSessionPtr = std::shared_ptr<TestServerSession>;

/** Mock database instance class  */
class TestDatabaseInstance : public DatabaseInstance {
public:
    TestDatabaseInstance(Session::Type type,
                         const std::string &hostname,
                         const std::string &prefix,
                         int port)
        : DatabaseInstance(type, hostname, prefix, port)
    {
    }

    /** override allocate session (create a TestServerSession) to avoid creating a connection */
    ServerSessionPtr allocate_session(
        ProxyServerPtr server,
        UserPtr user,
        uint64_t db_id,
        const std::unordered_map<std::string, std::string> &parameters) override
    {
        return std::make_shared<TestServerSession>(Session::Type::PRIMARY, _session_id++, db_id,
                                                   "db" + std::to_string(db_id), user->username());
    }

private:
    uint64_t _session_id = 0;
};
using TestDatabaseInstancePtr = std::shared_ptr<TestDatabaseInstance>;

/** Mock database set derived class */
class TestableDatabaseSet : public DatabaseSet {
public:
    TestableDatabaseSet() : DatabaseSet(5) {}

    /** override abstract method */
    ServerSessionPtr allocate_session(
        ProxyServerPtr server,
        UserPtr user,
        uint64_t db_id,
        const std::unordered_map<std::string, std::string> &parameters) override
    {
        // do nothing, not used in this test
        return nullptr;
    }

    /** allocate session from Test Instance and set the session's instance */
    ServerSessionPtr allocate_session(uint64_t db_id,
                                      const std::string &username,
                                      DatabaseInstancePtr instance)
    {
        auto session =
            _allocate_session(nullptr, std::make_shared<User>(username), db_id, {}, instance);
        TestServerSessionPtr test_session = std::dynamic_pointer_cast<TestServerSession>(session);
        test_session->set_instance(instance);
        return session;
    }

    /** override abstract method */
    void release_session(ServerSessionPtr session, bool deallocate) override
    {
        DatabaseSet::_release_session(session, 1, deallocate);
    }

    /** make public for testing */
    DatabaseInstancePtr get_least_loaded_instance() { return _get_least_loaded_instance(); }

    /** make public for testing */
    std::map<DatabaseInstancePtr, int> get_instance_sessions() { return _get_instance_sessions(); }
};
using TestableDatabaseSetPtr = std::shared_ptr<TestableDatabaseSet>;

/** Test DatabasePool Class */
class DatabasePoolTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() { springtail_init(); }

    void SetUp() override
    {
        pool = std::make_shared<DatabasePool>();
        // Create 3 sessions: type, id, db_id, database name, username
        session1 =
            std::make_shared<TestServerSession>(Session::Type::PRIMARY, 1, 1, "db1", "user1");
        session2 =
            std::make_shared<TestServerSession>(Session::Type::PRIMARY, 2, 1, "db1", "user1");
        session3 =
            std::make_shared<TestServerSession>(Session::Type::PRIMARY, 3, 2, "db2", "user1");
        session4 =
            std::make_shared<TestServerSession>(Session::Type::PRIMARY, 4, 2, "db2", "user2");

        instance1 =
            std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5432);
        instance2 =
            std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5433);

        session1->set_instance(instance1);
        session2->set_instance(instance1);
        session3->set_instance(instance2);
        session4->set_instance(instance2);
    }

    DatabasePoolPtr pool;
    TestServerSessionPtr session1;
    TestServerSessionPtr session2;
    TestServerSessionPtr session3;
    TestServerSessionPtr session4;

    TestDatabaseInstancePtr instance1;
    TestDatabaseInstancePtr instance2;
};

TEST_F(DatabasePoolTest, AddGetSession)
{
    pool->add_session(session1);
    pool->add_session(session3);
    pool->add_session(session4);

    EXPECT_EQ(pool->size(), 3);

    auto retrieved_session = pool->get_session(1, "user1");
    EXPECT_EQ(retrieved_session, session1);
    EXPECT_EQ(pool->size(), 2);

    retrieved_session = pool->get_session(2, "user1");
    EXPECT_EQ(retrieved_session, session3);
    EXPECT_EQ(pool->size(), 1);

    retrieved_session = pool->get_session(2, "user2");
    EXPECT_EQ(retrieved_session, session4);
    EXPECT_EQ(pool->size(), 0);

    auto null_session = pool->get_session(2, "user2");
    EXPECT_EQ(null_session, nullptr);
}

TEST_F(DatabasePoolTest, EvictByDbId)
{
    pool->add_session(session1);
    pool->add_session(session2);
    pool->add_session(session3);
    pool->add_session(session4);

    pool->evict(1);

    EXPECT_EQ(pool->size(), 2);

    auto null_session = pool->get_session(1, "user1");
    EXPECT_EQ(null_session, nullptr);

    auto non_null_session = pool->get_session(2, "user1");
    EXPECT_NE(non_null_session, nullptr);
}

TEST_F(DatabasePoolTest, EvictBySession)
{
    pool->add_session(session1);
    pool->add_session(session2);
    pool->add_session(session3);
    pool->add_session(session4);
    pool->evict(session3);

    EXPECT_EQ(pool->size(), 3);

    auto null_session = pool->get_session(2, "user1");
    EXPECT_EQ(null_session, nullptr);
}

TEST_F(DatabasePoolTest, EvictByInstance)
{
    pool->add_session(session1);
    pool->add_session(session2);
    pool->add_session(session3);
    pool->add_session(session4);
    pool->evict(instance2);

    EXPECT_EQ(pool->size(), 2);

    auto null_session = pool->get_session(2, "user1");
    EXPECT_EQ(null_session, nullptr);

    auto non_null_session = pool->get_session(1, "user1");
    EXPECT_NE(non_null_session, nullptr);
}

TEST_F(DatabasePoolTest, AddMultipleSessions)
{
    pool->add_session(session1);
    pool->add_session(session2);
    auto retrieved_session1 = pool->get_session(1, "user1");
    auto retrieved_session2 = pool->get_session(1, "user1");
    EXPECT_NE(retrieved_session1, retrieved_session2);
    EXPECT_TRUE((retrieved_session1 == session1 && retrieved_session2 == session2) ||
                (retrieved_session1 == session2 && retrieved_session2 == session1));
}

/** Test DatabaseSet class */
class DatabaseSetTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() { springtail_init(); }

    void SetUp() override
    {
        db_set = std::make_shared<TestableDatabaseSet>();
        instance1 =
            std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5432);
        instance2 =
            std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5433);
    }

    std::shared_ptr<TestableDatabaseSet> db_set;
    TestDatabaseInstancePtr instance1;
    TestDatabaseInstancePtr instance2;
};

TEST_F(DatabaseSetTest, AllocateAndReleaseSession)
{
    auto session = db_set->allocate_session(1, "user1", instance1);
    EXPECT_NE(session, nullptr);
    EXPECT_EQ(session->database_id(), 1);
    EXPECT_EQ(session->username(), "user1");

    db_set->release_session(session, false);

    EXPECT_EQ(db_set->pool_size(), 1);

    auto retrieved_session = db_set->get_session(1, "user1");
    EXPECT_EQ(retrieved_session, session);

    EXPECT_EQ(db_set->pool_size(), 0);
}

TEST_F(DatabaseSetTest, AllocateMultipleSessions)
{
    auto session1 = db_set->allocate_session(1, "user1", instance1);
    auto session2 = db_set->allocate_session(1, "user1", instance1);
    EXPECT_NE(session1, session2);

    db_set->release_session(session1, false);
    db_set->release_session(session2, false);

    auto retrieved_session1 = db_set->get_session(1, "user1");
    auto retrieved_session2 = db_set->get_session(1, "user1");
    EXPECT_NE(retrieved_session1, retrieved_session2);
    EXPECT_TRUE((retrieved_session1 == session1 && retrieved_session2 == session2) ||
                (retrieved_session1 == session2 && retrieved_session2 == session1));

    db_set->release_session(retrieved_session1, true);
    db_set->release_session(retrieved_session2, true);

    EXPECT_EQ(db_set->pool_size(), 0);

    auto null_session = db_set->get_session(1, "user1");
    EXPECT_EQ(null_session, nullptr);
}

TEST_F(DatabaseSetTest, RemoveInstance)
{
    auto session1 = db_set->allocate_session(1, "user1", instance1);
    auto session2 = db_set->allocate_session(1, "user2", instance1);
    auto session3 = db_set->allocate_session(2, "user1", instance2);
    db_set->release_session(session1, false);
    db_set->release_session(session2, false);
    db_set->release_session(session3, false);

    EXPECT_EQ(db_set->pool_size(), 3);
    db_set->remove_instance(instance1);
    EXPECT_EQ(db_set->pool_size(), 1);

    auto null_session1 = db_set->get_session(1, "user1");
    auto null_session2 = db_set->get_session(1, "user2");
    EXPECT_EQ(null_session1, nullptr);
    EXPECT_EQ(null_session2, nullptr);
}

TEST_F(DatabaseSetTest, RemoveDatabase)
{
    auto session1 = db_set->allocate_session(1, "user1", instance1);
    auto session2 = db_set->allocate_session(2, "user1", instance2);
    db_set->release_session(session1, false);
    db_set->release_session(session2, false);

    db_set->remove_database(1);

    auto null_session = db_set->get_session(1, "user1");
    auto non_null_session = db_set->get_session(2, "user1");
    EXPECT_EQ(null_session, nullptr);
    EXPECT_NE(non_null_session, nullptr);
}

TEST_F(DatabaseSetTest, GetLeastLoadedInstance)
{
    auto session1 = db_set->allocate_session(1, "user1", instance1);
    auto session2 = db_set->allocate_session(2, "user1", instance2);
    auto session3 = db_set->allocate_session(2, "user1", instance2);
    db_set->release_session(session1, false);
    db_set->release_session(session2, false);
    db_set->release_session(session3, false);

    auto instance_sessions = db_set->get_instance_sessions();
    EXPECT_EQ(instance_sessions.size(), 2);

    for (auto itr = instance_sessions.begin(); itr != instance_sessions.end(); ++itr) {
        if (itr->first == instance1) {
            EXPECT_EQ(itr->second, 1);
        } else if (itr->first == instance2) {
            EXPECT_EQ(itr->second, 2);
        }
    }

    instance_sessions = db_set->get_instance_sessions();
    EXPECT_EQ(instance_sessions.size(), 2);

    auto least_loaded_instance = db_set->get_least_loaded_instance();
    EXPECT_EQ(least_loaded_instance, instance1);
}

}  // namespace