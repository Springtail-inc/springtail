#include <gtest/gtest.h>

#include <common/init.hh>

#include <proxy/database.hh>
#include <proxy/server_session.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

namespace {

    class TestEnvironment : public ::testing::Environment {
    public:
        virtual void SetUp() {
            springtail_init_test();
        }
        virtual void TearDown() {
            springtail_shutdown();
        }
    };

    /** Mock server session */
    class TestServerSession : public ServerSession {
    public:
        TestServerSession(Session::Type type, uint64_t id, uint64_t db_id, const std::string &database, const std::string &username)
            : ServerSession(type, id, db_id, database, username) {}

        /** override for testing, connection is always active */
        bool is_connection_closed() const override {
            return false;
        }

        /** helper to set the db instance */
        void set_instance(DatabaseInstancePtr instance) {
            assert(instance != nullptr);
            _instance = instance;
        }
    };
    using TestServerSessionPtr = std::shared_ptr<TestServerSession>;

    /** Mock database instance class  */
    class TestDatabaseInstance : public DatabaseInstance {
    public:

        TestDatabaseInstance(Session::Type type, const std::string &hostname, const std::string &prefix, int port)
            : DatabaseInstance({0, 0, 0}, type, hostname, port, prefix) {}

        /** override allocate session (create a TestServerSession) to avoid creating a connection */
        ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database) override
        {
            return std::make_shared<TestServerSession>(Session::Type::PRIMARY, _session_id++, db_id,
                "db" + std::to_string(db_id), user->username());
        }

    private:
        uint64_t _session_id = 0;
    };
    using TestDatabaseInstancePtr = std::shared_ptr<TestDatabaseInstance>;


    /** Mock database set derived class */
    class TestableDatabaseSet : public DatabaseInstanceSet {
    public:
        TestableDatabaseSet() : DatabaseInstanceSet(5) {}

        /** override abstract method */
        ServerSessionPtr allocate_session(UserPtr user,
                uint64_t db_id,
                const std::unordered_map<std::string, std::string> &parameters,
                const std::string &database) override
        {
            // do nothing, not used in this test
            return nullptr;
        }

        /** allocate session from Test Instance and set the session's instance */
        ServerSessionPtr allocate_session(uint64_t db_id, const std::string &username, DatabaseInstancePtr instance, const std::string &database)
        {
            std::unique_lock lock(_base_mutex);
            auto session = _allocate_session(std::make_shared<User>(username), db_id, {}, instance, database);
            TestServerSessionPtr test_session = std::dynamic_pointer_cast<TestServerSession>(session);
            test_session->set_instance(instance);
            return session;
        }

        /** override abstract method */
        void
        release_session(ServerSessionPtr session, bool deallocate) override
        {
            std::shared_lock lock(_base_mutex);
            DatabaseInstanceSet::_release_session(session, deallocate);
        }

        /** make public for testing */
        DatabaseInstancePtr
        get_least_loaded_instance()
        {
            std::shared_lock lock(_base_mutex);
            return _get_least_loaded_instance();
        }

        /** override abstract method */
        void
        release_expired_sessions() override
        {
            std::unique_lock lock(_base_mutex);
            for (auto instance: _active_instances) {
                instance->get_pool()->evict_expired_sessions();
            }
        }

        /** make public for testing */
        std::map<DatabaseInstancePtr, int>
        get_instance_sessions() {
            std::shared_lock lock(_base_mutex);
            return _get_instance_sessions();
        }
    };
    using TestableDatabaseSetPtr = std::shared_ptr<TestableDatabaseSet>;


    /** Test DatabasePool Class */
    class DatabasePoolTest : public ::testing::Test {
    protected:
        void SetUp() override
        {
            DatabasePool::PoolConfig pool_config = {0, 0, 0};
            pool = std::make_shared<DatabasePool>(pool_config);

            // Create 4 sessions: type, id, db_id, database name, username
            session1 = std::make_shared<TestServerSession>(Session::Type::PRIMARY, 1, 1, "db1", "user1");
            session2 = std::make_shared<TestServerSession>(Session::Type::PRIMARY, 2, 1, "db1", "user1");
            session3 = std::make_shared<TestServerSession>(Session::Type::PRIMARY, 3, 2, "db2", "user1");
            session4 = std::make_shared<TestServerSession>(Session::Type::PRIMARY, 4, 2, "db2", "user2");

            instance1 = std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5432);
            instance2 = std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5433);

            session1->set_instance(instance1);
            session2->set_instance(instance1);
            session3->set_instance(instance2);
            session4->set_instance(instance2);
        }

        void create_sessions(std::vector<uint64_t> &db_ids, std::vector<std::string> &user_names, std::vector<TestServerSessionPtr> &sessions)
        {
            uint64_t id = 0;
            for (auto &db_id: db_ids) {
                for (auto &user_name: user_names) {
                    id++;
                    TestServerSessionPtr new_session = std::make_shared<TestServerSession>(Session::Type::PRIMARY, id, db_id, "db" + std::to_string(db_id), user_name);
                    sessions.push_back(new_session);
                }
            }
            EXPECT_EQ(sessions.size(), db_ids.size() * user_names.size());
        }

        void add_sessions(DatabasePoolPtr pool,
                          std::vector<TestServerSessionPtr>::iterator it_begin,
                          std::vector<TestServerSessionPtr>::iterator it_end)
        {
            for (auto &it = it_begin; it != it_end; ++it) {
                pool->add_session(*it);
            }
            EXPECT_LE(pool->size(), pool->get_size_limit());
        }

        void test_timeout_limit(std::vector<uint64_t> &db_ids, std::vector<std::string> &user_names)
        {
            DatabasePool::PoolConfig pool_config = {10, 5, 3};
            DatabasePoolPtr pool = std::make_shared<DatabasePool>(pool_config);

            EXPECT_EQ(pool->get_size_limit(), 10);
            EXPECT_EQ(pool->get_timeout_limit(), 5);

            std::vector<TestServerSessionPtr> sessions;

            create_sessions(db_ids, user_names, sessions);
            add_sessions(pool, sessions.begin(), sessions.end());
            EXPECT_EQ(pool->size(), std::min(db_ids.size() * user_names.size(), pool->get_size_limit()));

            sleep(4);

            pool->evict_expired_sessions();
            EXPECT_EQ(pool->size(), pool->get_timeout_limit());

            TestServerSessionPtr next_session1 = std::make_shared<TestServerSession>(Session::Type::PRIMARY, 10, 1, "db1", "user1");
            pool->add_session(next_session1);
            EXPECT_EQ(pool->size(), pool->get_timeout_limit() + 1);

            pool->evict_expired_sessions();
            EXPECT_EQ(pool->size(), pool->get_timeout_limit());

            TestServerSessionPtr next_session2 = std::make_shared<TestServerSession>(Session::Type::PRIMARY, 10, 1, "db1", "user2");
            pool->add_session(next_session2);
            EXPECT_EQ(pool->size(), pool->get_timeout_limit() + 1);

            std::vector<TestServerSessionPtr> more_sessions;
            create_sessions(db_ids, user_names, more_sessions);
            add_sessions(pool, more_sessions.begin(), more_sessions.end());
            EXPECT_EQ(pool->size(), std::min(db_ids.size() * user_names.size() + 2, pool->get_size_limit()));
        }

        DatabasePoolPtr pool;
        TestServerSessionPtr session1;
        TestServerSessionPtr session2;
        TestServerSessionPtr session3;
        TestServerSessionPtr session4;

        TestDatabaseInstancePtr instance1;
        TestDatabaseInstancePtr instance2;
    };

    TEST_F(DatabasePoolTest, AddGetSession) {
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

    TEST_F(DatabasePoolTest, EvictByDbId) {
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

    TEST_F(DatabasePoolTest, TestAddGetWithLimit) {
        DatabasePool::PoolConfig pool_config = {10, 0, 0};
        DatabasePoolPtr pool = std::make_shared<DatabasePool>(pool_config);

        std::vector<std::string> users = {"user1", "user2", "user3", "user4"};
        std::vector<uint64_t> database_ids = {1, 2, 3 ,4};

        std::vector<TestServerSessionPtr> sessions;

        create_sessions(database_ids, users, sessions);
        add_sessions(pool, sessions.begin(), sessions.end());
        EXPECT_EQ(pool->size(), pool->get_size_limit());

        for (auto &user: users) {
            EXPECT_EQ(pool->get_session(1, user), nullptr);
        }
        EXPECT_EQ(pool->get_session(2, "user1"), nullptr);
        EXPECT_EQ(pool->get_session(2, "user2"), nullptr);
        ServerSessionPtr session = pool->get_session(2, "user3");
        EXPECT_EQ(session, sessions[6]);
        session = pool->get_session(2, "user4");
        EXPECT_EQ(session, sessions[7]);

        EXPECT_EQ(pool->size(), 8);
    }

    TEST_F(DatabasePoolTest, TestAddGetWithTimeoutLimit) {
        std::vector<std::string> users = {"user1", "user2", "user3"};
        std::vector<uint64_t> database_ids = {1, 2, 3};
        test_timeout_limit(database_ids, users);
    }

    TEST_F(DatabasePoolTest, TestAddGetWithSizeAndTimeoutLimit) {
        std::vector<std::string> users = {"user1", "user2", "user3", "user4"};
        std::vector<uint64_t> database_ids = {1, 2, 3, 4};
        test_timeout_limit(database_ids, users);
    }

    TEST_F(DatabasePoolTest, TestDatabaseUserLIFO) {
        DatabasePool::PoolConfig pool_config = {10, 0, 0};
        DatabasePoolPtr pool = std::make_shared<DatabasePool>(pool_config);

        std::vector<std::string> users = {"user1", "user1", "user1", "user1"};
        std::vector<uint64_t> database_ids = {1, 2};

        std::vector<TestServerSessionPtr> sessions;

        create_sessions(database_ids, users, sessions);
        add_sessions(pool, sessions.begin(), sessions.end());
        EXPECT_EQ(pool->size(), 8);
        EXPECT_EQ(pool->size(1, "user1"), 4);
        EXPECT_EQ(pool->size(2, "user1"), 4);

        EXPECT_EQ(pool->get_session(1, "user1"), sessions[3]);
        EXPECT_EQ(pool->get_session(1, "user1"), sessions[2]);
        EXPECT_EQ(pool->get_session(1, "user1"), sessions[1]);
        EXPECT_EQ(pool->get_session(1, "user1"), sessions[0]);

        EXPECT_EQ(pool->get_session(2, "user1"), sessions[7]);
        EXPECT_EQ(pool->get_session(2, "user1"), sessions[6]);
        EXPECT_EQ(pool->get_session(2, "user1"), sessions[5]);
        EXPECT_EQ(pool->get_session(2, "user1"), sessions[4]);

        EXPECT_EQ(pool->size(), 0);
    }

    TEST_F(DatabasePoolTest, TestDatabaseEvict) {
        DatabasePool::PoolConfig pool_config = {10, 0, 0};
        DatabasePoolPtr pool = std::make_shared<DatabasePool>(pool_config);

        std::vector<std::string> users = {"user1", "user1", "user1", "user1"};
        std::vector<uint64_t> database_ids = {1, 2};

        std::vector<TestServerSessionPtr> sessions;

        create_sessions(database_ids, users, sessions);
        add_sessions(pool, sessions.begin(), sessions.end());
        EXPECT_EQ(pool->size(), 8);

        pool->evict(2);
        EXPECT_EQ(pool->size(), 4);
        EXPECT_EQ(pool->get_session(2, "user1"), nullptr);

        pool->evict(1);
        EXPECT_EQ(pool->size(), 0);
        EXPECT_EQ(pool->get_session(1, "user1"), nullptr);
    }

    TEST_F(DatabasePoolTest, AddMultipleSessions) {
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
        void SetUp() override
        {
            db_set = std::make_shared<TestableDatabaseSet>();
            instance1 = std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5432);
            instance2 = std::make_shared<TestDatabaseInstance>(Session::Type::PRIMARY, "localhost", "", 5433);
        }

        std::shared_ptr<TestableDatabaseSet> db_set;
        TestDatabaseInstancePtr instance1;
        TestDatabaseInstancePtr instance2;
    };

    TEST_F(DatabaseSetTest, AllocateAndReleaseSession) {
        auto session = db_set->allocate_session(1, "user1", instance1, "springtail");
        EXPECT_NE(session, nullptr);
        EXPECT_EQ(session->database_id(), 1);
        EXPECT_EQ(session->username(), "user1");

        EXPECT_EQ(db_set->get_pooled_session(1, "user1"), nullptr);

        db_set->release_session(session, false);

        EXPECT_EQ(instance1->get_pool()->size(), 1);
        EXPECT_EQ(instance1->get_pool()->size(1, "user1"), 1);

        auto retrieved_session = db_set->get_pooled_session(1, "user1");
        EXPECT_EQ(retrieved_session, session);

        EXPECT_EQ(instance1->get_pool()->size(), 0);
        EXPECT_EQ(instance1->get_pool()->size(1, "user1"), 0);
    }

    TEST_F(DatabaseSetTest, AllocateMultipleSessions) {
        auto session1 = db_set->allocate_session(1, "user1", instance1, "springtail");
        auto session2 = db_set->allocate_session(1, "user1", instance1, "springtail");
        EXPECT_NE(session1, session2);

        db_set->release_session(session1, false);
        db_set->release_session(session2, false);

        auto retrieved_session1 = db_set->get_pooled_session(1, "user1");
        auto retrieved_session2 = db_set->get_pooled_session(1, "user1");
        EXPECT_NE(retrieved_session1, retrieved_session2);
        EXPECT_TRUE(retrieved_session1 == session2);
        EXPECT_TRUE(retrieved_session2 == session1);

        db_set->release_session(retrieved_session1, true);
        db_set->release_session(retrieved_session2, true);

        EXPECT_EQ(instance1->get_pool()->size(), 0);

        auto null_session = db_set->get_pooled_session(1, "user1");
        EXPECT_EQ(null_session, nullptr);
    }

    TEST_F(DatabaseSetTest, RemoveDatabase) {
        auto session1 = db_set->allocate_session(1, "user1", instance1, "springtail");
        auto session2 = db_set->allocate_session(2, "user1", instance2, "springtail");
        db_set->release_session(session1, false);
        db_set->release_session(session2, false);

        db_set->remove_database(1);

        auto null_session = db_set->get_pooled_session(1, "user1");
        auto non_null_session = db_set->get_pooled_session(2, "user1");
        EXPECT_EQ(null_session, nullptr);
        EXPECT_NE(non_null_session, nullptr);
    }

    TEST_F(DatabaseSetTest, GetLeastLoadedInstance) {
        auto session1 = db_set->allocate_session(1, "user1", instance1, "springtail");
        auto session2 = db_set->allocate_session(2, "user1", instance2, "springtail");
        auto session3 = db_set->allocate_session(2, "user1", instance2, "springtail");
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

} // namespace

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    // gtest takes ownership of the TestEnvironment ptr - we don't delete it.
    ::testing::AddGlobalTestEnvironment(new TestEnvironment());
    return RUN_ALL_TESTS();
}