#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <common/init.hh>
#include <common/properties.hh>

#include <proxy/database.hh>
#include <proxy/server_session.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

namespace {

    class TestEnvironment : public ::testing::Environment {
    public:
        void SetUp() override {
            springtail_init_test(LOG_PROXY);
        }
        void TearDown() override {
            springtail_shutdown();
        }
    };

    /** Mock server session for testing */
    class MockServerSession : public ServerSession {
    public:
        MockServerSession(Session::Type type, uint64_t id, uint64_t db_id, const std::string &database, const std::string &username)
            : ServerSession(type, id, db_id, database, username), _connection_closed(false) {}

        /** Override for testing - control connection state */
        bool is_connection_closed() const override {
            return _connection_closed;
        }

        /** Helper to simulate connection closure */
        void set_connection_closed(bool closed) {
            _connection_closed = closed;
        }

        /** Helper to set the database instance */
        void set_instance(DatabaseInstancePtr instance) {
            _instance = instance;
        }

    private:
        bool _connection_closed;
    };
    using MockServerSessionPtr = std::shared_ptr<MockServerSession>;

    /** Test database instance that creates mock sessions */
    class TestDatabaseInstance : public DatabaseInstance {
    public:
        TestDatabaseInstance(const std::string &hostname, int port, const std::string &replica_id)
            : DatabaseInstance(DatabasePool::PoolConfig({10, 5, 300}), Session::Type::REPLICA, hostname, port, "", replica_id),
              _session_id_counter(1) {}

        /** Override to create mock sessions instead of real connections */
        ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database) override
        {
            auto session = std::make_shared<MockServerSession>(
                Session::Type::REPLICA, _session_id_counter++, db_id, database, user->username());
            session->set_instance(shared_from_this());

            {
                std::unique_lock lock(_active_sessions_mutex);
                _active_sessions[session->id()] = session;
            }

            std::cout << "Allocated MockServerSession ID " << session->id() << " for user " << user->username()
                      << " on database " << database << " from instance " << replica_id() << std::endl;

            EXPECT_TRUE(session->get_instance() != nullptr);
            return session;
        }

    private:
        uint64_t _session_id_counter;
    };
    using TestDatabaseInstancePtr = std::shared_ptr<TestDatabaseInstance>;

    /** Testable DatabaseReplicaSet that exposes protected methods */
    class TestableReplicaSet : public DatabaseReplicaSet {
    public:
        TestableReplicaSet() : DatabaseReplicaSet(DatabasePool::PoolConfig({10, 5, 300})) {}

        // override allocate_session to always use replica1 for testing
        // also allocate using the TestDatabaseInstance
        ServerSessionPtr allocate_session_on_replica(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database,
            const std::string &replica_id)
        {
            // try to use replica1 first, then replica2 if not found
            auto instance = get_replica_instance(replica_id);
            if (!instance) {
                return nullptr;
            }

            TestDatabaseInstancePtr replica = std::dynamic_pointer_cast<TestDatabaseInstance>(instance);
            auto session = replica->allocate_session(user, db_id, parameters, database);

            return session;
        }

        ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database) override
        {
            return allocate_session_on_replica(user, db_id, parameters, database, "replica1");
        }

        /** Add a test replica instance directly */
        void add_test_replica(const std::string &replica_id, TestDatabaseInstancePtr instance) {
            add_instance(instance);
        }

        /** Get replica count for testing */
        size_t get_replica_count() const {
            std::shared_lock lock(_base_mutex);
            return _active_instances.size();
        }

        /** Get shutdown pending count for testing */
        size_t get_shutdown_pending_count() const {
            std::shared_lock lock(_base_mutex);
            return _shutdown_pending_instances.size();
        }

        /** Check if replica is in active set */
        bool has_active_replica(const std::string &replica_id) const {
            std::shared_lock lock(_base_mutex);
            for (const auto &instance : _active_instances) {
                if (instance->replica_id() == replica_id) {
                    return true;
                }
            }
            return false;
        }

        /** Check if replica is in shutdown pending set */
        bool has_shutdown_pending_replica(DatabaseInstancePtr replica) const {
            std::shared_lock lock(_base_mutex);
            return _shutdown_pending_instances.contains(replica);
        }

        /** Get instance session count for testing */
        int get_instance_session_count(DatabaseInstancePtr instance) const {
            std::shared_lock lock(_base_mutex);
            return instance->all_session_count();
        }

        /** Check if instance exists in tracking maps */
        bool has_instance_in_tracking(DatabaseInstancePtr instance) const {
            std::shared_lock lock(_base_mutex);
            return _active_instances.contains(instance);
        }

        /** Manually remove instance for testing cleanup scenarios */
        void test_remove_instance(DatabaseInstancePtr instance) {
            instance->initiate_shutdown();
        }
    };
    using TestableReplicaSetPtr = std::shared_ptr<TestableReplicaSet>;

    /** Test suite for DatabaseReplicaSet shutdown functionality */
    class ReplicaShutdownTest : public ::testing::Test {
    protected:
        void SetUp() override {
            replica_set = std::make_shared<TestableReplicaSet>();
            test_user = std::make_shared<User>("test_user");

            // Create test replica instances
            replica1 = std::make_shared<TestDatabaseInstance>("replica1.example.com", 5432, "replica1");
            replica2 = std::make_shared<TestDatabaseInstance>("replica2.example.com", 5432, "replica2");

            // Add replicas to the set
            replica_set->add_test_replica("replica1", replica1);
            replica_set->add_test_replica("replica2", replica2);
        }

        void TearDown() override {
            // Clean up any remaining instances
            if (replica_set->has_instance_in_tracking(replica1)) {
                replica_set->test_remove_instance(replica1);
            }
            if (replica_set->has_instance_in_tracking(replica2)) {
                replica_set->test_remove_instance(replica2);
            }
        }

        TestableReplicaSetPtr replica_set;
        UserPtr test_user;
        TestDatabaseInstancePtr replica1;
        TestDatabaseInstancePtr replica2;
    };

    TEST_F(ReplicaShutdownTest, InitialState) {
        // Verify initial setup
        EXPECT_EQ(replica_set->get_replica_count(), 2);
        EXPECT_EQ(replica_set->get_shutdown_pending_count(), 0);
        EXPECT_TRUE(replica_set->has_active_replica("replica1"));
        EXPECT_TRUE(replica_set->has_active_replica("replica2"));
        EXPECT_TRUE(replica1->is_active());
        EXPECT_TRUE(replica2->is_active());
    }

    TEST_F(ReplicaShutdownTest, InitiateShutdownWithoutSessions) {
        // Test initiating shutdown when replica has no active sessions
        EXPECT_TRUE(replica1->is_active());
        EXPECT_EQ(replica1->pooled_session_count(), 0);

        // Initiate shutdown
        replica_set->initiate_replica_shutdown("replica1");

        // Verify state changes
        EXPECT_FALSE(replica1->is_active()); // Instance should be marked inactive
        EXPECT_FALSE(replica_set->has_active_replica("replica1")); // Removed from active replicas
        EXPECT_FALSE(replica_set->has_shutdown_pending_replica(replica1)); // No active sessions so was removed immediately
        EXPECT_EQ(replica_set->get_replica_count(), 1); // Only replica2 remains active
        EXPECT_EQ(replica_set->get_shutdown_pending_count(), 0);
    }

    TEST_F(ReplicaShutdownTest, InitiateShutdownWithPooledSessions) {
        // Create and pool some sessions
        std::vector<ServerSessionPtr> sessions;
        for (int i = 0; i < 3; ++i) {
            auto session = replica_set->allocate_session(test_user, 1, {}, "test_db");
            ASSERT_NE(session, nullptr);
            ASSERT_NE(session->get_instance(), nullptr);
            sessions.push_back(session);
        }

        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 3);
        EXPECT_EQ(replica1->pooled_session_count(), 0);
        EXPECT_EQ(replica1->active_session_count(), 3);
        EXPECT_EQ(replica1->all_session_count(), 3);

        // Release sessions to pool
        for (auto& session : sessions) {
            replica_set->release_session(session, false); // Don't deallocate - add to pool
        }
        sessions.clear();

        // Verify sessions are in pool
        EXPECT_EQ(replica1->all_session_count(), 3);
        EXPECT_EQ(replica1->pooled_session_count(), 3);
        EXPECT_EQ(replica1->active_session_count(), 0); // All sessions should be removed

        // Initiate shutdown
        replica_set->initiate_replica_shutdown("replica1");

        // Verify shutdown behavior
        EXPECT_EQ(replica1->all_session_count(), 0);
        EXPECT_FALSE(replica1->is_active());
        EXPECT_FALSE(replica_set->has_active_replica("replica1"));
        EXPECT_FALSE(replica_set->has_shutdown_pending_replica(replica1));
        EXPECT_EQ(replica_set->get_replica_count(), 1); // Only replica2 remains active
    }

    TEST_F(ReplicaShutdownTest, InitiateShutdownWithActiveSessions) {
        // Allocate active sessions (not yet returned to pool)
        std::vector<ServerSessionPtr> active_sessions;
        for (int i = 0; i < 3; ++i) {
            auto session = replica_set->allocate_session(test_user, 1, {}, "test_db");
            ASSERT_NE(session, nullptr);
            active_sessions.push_back(session);
        }

        // Verify active session tracking
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 3);
        EXPECT_EQ(replica1->pooled_session_count(), 0); // No pooled sessions yet

        // Initiate shutdown
        replica_set->initiate_replica_shutdown("replica1");

        // Verify shutdown state
        EXPECT_FALSE(replica1->is_active());
        EXPECT_FALSE(replica_set->has_active_replica("replica1"));
        EXPECT_TRUE(replica_set->has_shutdown_pending_replica(replica1));

        // Pool should be empty (was already empty, remains empty)
        EXPECT_EQ(replica1->pooled_session_count(), 0);

        // Active sessions should still be tracked
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 3);

        // Now release the active sessions - they should be deallocated instead of pooled
        for (auto& session : active_sessions) {
            replica_set->release_session(session, false); // Try to pool, but should be deallocated
        }
        active_sessions.clear();

        // Verify sessions were deallocated instead of pooled
        EXPECT_EQ(replica1->pooled_session_count(), 0);
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 0);

        // Instance should be fully removed from tracking when session count reaches 0
        EXPECT_FALSE(replica_set->has_instance_in_tracking(replica1));
        EXPECT_FALSE(replica_set->has_shutdown_pending_replica(replica1));
    }

    TEST_F(ReplicaShutdownTest, PooledSessionRetrievalExcludesShutdownPendingInstances) {
        // Create sessions on replica1 and pool them
        for (int i = 0; i < 3; ++i) {
            auto session = replica_set->allocate_session(test_user, 1, {}, "test_db");
            ASSERT_NE(session, nullptr);
            replica_set->release_session(session, false); // Pool the session
        }

        // Create sessions on replica2 and pool them
        for (int i = 0; i < 2; ++i) {
            auto session = replica_set->allocate_session_on_replica(test_user, 1, {}, "test_db", "replica2");
            ASSERT_NE(session, nullptr);
            replica_set->release_session(session, false); // Pool the session
        }

        // Verify both pools have sessions
        EXPECT_EQ(replica1->pooled_session_count(), 3);
        EXPECT_EQ(replica2->pooled_session_count(), 2);

        // Initiate shutdown on replica1
        replica_set->initiate_replica_shutdown("replica1");

        // Try to get pooled sessions - should only come from replica2
        for (int i = 0; i < 2; ++i) {
            auto session = replica_set->get_pooled_session(1, "test_user");
            ASSERT_NE(session, nullptr);

            // Verify session came from replica2, not shutdown-pending replica1
            auto instance = session->get_instance();
            EXPECT_EQ(instance, replica2);
            EXPECT_NE(instance, replica1);

            // Clean up
            replica_set->release_session(session, true);
        }

        // Verify replica1's pool was cleared during shutdown
        EXPECT_EQ(replica1->pooled_session_count(), 0);
    }

    TEST_F(ReplicaShutdownTest, MultipleReplicasShutdown) {
        // Create sessions on both replicas
        std::vector<ServerSessionPtr> sessions1, sessions2;

        for (int i = 0; i < 2; ++i) {
            auto session1 = replica_set->allocate_session_on_replica(test_user, 1, {}, "test_db", "replica1");
            auto session2 = replica_set->allocate_session_on_replica(test_user, 1, {}, "test_db", "replica2");
            ASSERT_NE(session1, nullptr);
            ASSERT_NE(session2, nullptr);
            sessions1.push_back(session1);
            sessions2.push_back(session2);
        }

        // Initiate shutdown on both replicas
        replica_set->initiate_replica_shutdown("replica1");
        replica_set->initiate_replica_shutdown("replica2");

        EXPECT_EQ(replica_set->get_replica_count(), 0);

        // both should be in shutdown pending state
        EXPECT_EQ(replica_set->get_shutdown_pending_count(), 2);
        EXPECT_TRUE(replica_set->has_shutdown_pending_replica(replica1));
        EXPECT_TRUE(replica_set->has_shutdown_pending_replica(replica2));

        // Try to allocate new sessions - should fail since no active replicas
        auto session = replica_set->allocate_session(test_user, 1, {}, "test_db");
        EXPECT_EQ(session, nullptr);

        // Clean up sessions to complete shutdown
        for (auto& session : sessions1) {
            replica_set->release_session(session, true);
        }
        sessions1.clear();

        for (auto& session : sessions2) {
            replica_set->release_session(session, true);
        }
        sessions2.clear();

        // Both instances should be fully removed
        EXPECT_FALSE(replica_set->has_instance_in_tracking(replica1));
        EXPECT_FALSE(replica_set->has_instance_in_tracking(replica2));
        EXPECT_EQ(replica_set->get_shutdown_pending_count(), 0);
    }

#ifndef NDEBUG
    TEST_F(ReplicaShutdownTest, ShutdownNonExistentReplica) {
        // in debug mode this would abort with DCHECK
        EXPECT_DEATH(
            replica_set->initiate_replica_shutdown("non_existent_replica"), "replica != nullptr"
        );
    }
#endif

    TEST_F(ReplicaShutdownTest, MixedActiveAndPooledSessions) {
        // Create a mix of active and pooled sessions
        std::vector<ServerSessionPtr> active_sessions;

        // Create some active sessions
        for (int i = 0; i < 2; ++i) {
            auto session = replica_set->allocate_session(test_user, 1, {}, "test_db");
            ASSERT_NE(session, nullptr);
            active_sessions.push_back(session);
        }

        // Create and pool some sessions
        for (int i = 0; i < 3; ++i) {
            auto session = replica_set->allocate_session(test_user, 1, {}, "test_db");
            ASSERT_NE(session, nullptr);
            replica_set->release_session(session, false); // Pool it
        }

        // Verify state before shutdown
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 5); // 2 active + 3 pooled
        EXPECT_EQ(replica1->pooled_session_count(), 3);

        // Initiate shutdown
        replica_set->initiate_replica_shutdown("replica1");

        // Pool should be cleared immediately
        EXPECT_EQ(replica1->pooled_session_count(), 0);

        // Active sessions should still be tracked
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 2); // Only active sessions remain

        // Release active sessions - should be deallocated
        for (auto& session : active_sessions) {
            replica_set->release_session(session, false);
        }
        active_sessions.clear();

        // All sessions should be gone and instance removed
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 0);
        EXPECT_FALSE(replica_set->has_instance_in_tracking(replica1));
        EXPECT_FALSE(replica_set->has_shutdown_pending_replica(replica1));
    }

    TEST_F(ReplicaShutdownTest, SimulateRedisCallbackBehavior) {
        // This test simulates the behavior when _redis_fdw_change_cb receives DRAINING state

        // Create some active sessions to simulate real workload
        std::vector<ServerSessionPtr> sessions;
        for (int i = 0; i < 3; ++i) {
            auto session = replica_set->allocate_session(test_user, 1, {}, "test_db");
            ASSERT_NE(session, nullptr);
            sessions.push_back(session);
        }

        // Pool one session and keep two active
        replica_set->release_session(sessions.back(), false);
        sessions.pop_back();
        // sessions[1] and sessions[2] remain active

        // Verify initial state
        EXPECT_EQ(replica1->pooled_session_count(), 1);
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 3);
        EXPECT_TRUE(replica1->is_active());

        // Simulate the Redis callback for DRAINING state
        replica_set->initiate_replica_shutdown("replica1");

        // Verify immediate effects (similar to what happens in _redis_fdw_change_cb)
        EXPECT_FALSE(replica1->is_active()); // Instance marked inactive
        EXPECT_FALSE(replica_set->has_active_replica("replica1")); // Removed from active replicas
        EXPECT_TRUE(replica_set->has_shutdown_pending_replica(replica1)); // Added to shutdown pending
        EXPECT_EQ(replica1->pooled_session_count(), 0); // Pool cleared
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 2); // Only active sessions remain

        // Simulate gradual session completion (as would happen in production)
        replica_set->release_session(sessions.back(), false);
        sessions.pop_back();
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 1);
        EXPECT_TRUE(replica_set->has_shutdown_pending_replica(replica1)); // Still pending

        // Release final session
        replica_set->release_session(sessions.back(), false);
        sessions.pop_back();

        // Instance should be completely removed now
        EXPECT_EQ(replica_set->get_instance_session_count(replica1), 0);
        EXPECT_FALSE(replica_set->has_instance_in_tracking(replica1));
        EXPECT_FALSE(replica_set->has_shutdown_pending_replica(replica1));
        EXPECT_EQ(replica_set->get_shutdown_pending_count(), 0);
    }

} // namespace
