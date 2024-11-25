#include <gtest/gtest.h>

#include <common/common.hh>
#include <pg_repl/libpq_connection.hh>
#include <proxy/user_mgr.hh>

using namespace springtail;

namespace {
    static constexpr char USER_CREATE[] = "CREATE USER {} WITH LOGIN PASSWORD '{}'";
    static constexpr char USER_DROP[] = "DROP USER {}";
    static constexpr char USER_DROP_OWNED[] = "DROP OWNED BY {}";
    static constexpr char DATABASE_GRANT[] = "GRANT CONNECT ON DATABASE {} TO {}";
    static constexpr char DATABASE_REVOKE[] = "REVOKE CONNECT ON DATABASE {} FROM {}";
    static constexpr char CHANGE_PASSWORD[] = "ALTER USER {} WITH PASSWORD '{}'";
    static constexpr char CHANGE_ENCRYPTION[] = "SET PASSWORD_ENCRYPTION = '{}'";
    static constexpr char MD5_ENCRYPTION[] = "md5";
    static constexpr char SCRAM_ENCRYPTION[] = "SCRAM-SHA-256";

    class UserMgr_Test : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            springtail_init();

            pg_proxy::UserMgr *user_mgr = pg_proxy::UserMgr::get_instance();
            ASSERT_NE(user_mgr, nullptr);
            user_mgr->init([] {
                return std::optional<std::string>("springtail");
            }, _sleep_interval);
            user_mgr->start_thread();

            std::string host, user, password;
            int port;
            Properties::get_primary_db_config(host, port, user, password);
            std::string db_name = "springtail";
            _db_conn.connect(host, db_name, user, password, port, false);
        }
        static void TearDownTestSuite() {
            pg_proxy::UserMgr::get_instance()->stop_thread();
            pg_proxy::UserMgr::shutdown();
            _db_conn.disconnect();
        }
        void _add_user(const std::string &user, const std::string &password) {
            _db_conn.exec(fmt::format(USER_CREATE, user, password));
        }
        void _remove_user(const std::string &user) {
            _db_conn.exec(fmt::format(USER_DROP_OWNED, user));
            _db_conn.exec(fmt::format(USER_DROP, user));
        }
        void _add_database(const std::string &database) {
            _db_conn.exec(fmt::format(DATABASE_GRANT, database, "public"));
        }
        void _remove_database(const std::string &database) {
            _db_conn.exec(fmt::format(DATABASE_REVOKE, database, "public"));
        }
        void _change_user_password(const std::string &user, const std::string &password) {
            _db_conn.exec(fmt::format(CHANGE_PASSWORD, user, password));
        }
        void _change_encryption(const std::string &encryption) {
            _db_conn.exec(fmt::format(CHANGE_ENCRYPTION, encryption));
        }
        static inline springtail::LibPqConnection _db_conn;
        static inline uint32_t _sleep_interval = 1;
    };

    TEST_F(UserMgr_Test, TestAddRemoveUser) {
        pg_proxy::UserMgr *user_mgr = pg_proxy::UserMgr::get_instance();
        // add users
        _add_user("aaa", "aaa_password");
        _add_user("bbb", "bbb_password");
        sleep(2 * _sleep_interval);

        // verify access
        pg_proxy::UserPtr user = user_mgr->get_user("aaa", "springtail");
        ASSERT_NE(user, nullptr);
        user = user_mgr->get_user("bbb", "springtail");
        ASSERT_NE(user, nullptr);

        // remove users and verify access
        _remove_user("aaa");
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", "springtail");
        ASSERT_EQ(user, nullptr);
        _remove_user("bbb");
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("bbb", "springtail");
        ASSERT_EQ(user, nullptr);
    }

    TEST_F(UserMgr_Test, TestDatabaseChange) {
        pg_proxy::UserMgr *user_mgr = pg_proxy::UserMgr::get_instance();
        std::string db_name = "template1";

        // add user and verify access
        _add_user("aaa", "aaa_password");
        sleep(2 * _sleep_interval);
        pg_proxy::UserPtr user = user_mgr->get_user("aaa", db_name);
        ASSERT_NE(user, nullptr);

        // remove database and verify access
        _remove_database(db_name);
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", db_name);
        ASSERT_EQ(user, nullptr);

        // add database back and verify access
        _add_database(db_name);
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", db_name);
        ASSERT_NE(user, nullptr);

        // remove user and verify access
        _remove_user("aaa");
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", db_name);
        ASSERT_EQ(user, nullptr);
    }

    TEST_F(UserMgr_Test, TestUserPasswordChange) {
        pg_proxy::UserMgr *user_mgr = pg_proxy::UserMgr::get_instance();
        std::string db_name = "template1";

        // add user
        _add_user("aaa", "aaa_password");
        sleep(2 * _sleep_interval);
        pg_proxy::UserPtr user = user_mgr->get_user("aaa", db_name);
        ASSERT_NE(user, nullptr);

        std::string user_password = user->password();

        // change password
        _change_user_password("aaa", "aaabbb_password");
        sleep(2 * _sleep_interval);
        std::string new_user_password = user->password();
        ASSERT_NE(user_password, new_user_password);

        // remove user and verify access
        _remove_user("aaa");
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", db_name);
        ASSERT_EQ(user, nullptr);
    }

    TEST_F(UserMgr_Test, TestUserEncryptionChange) {
        pg_proxy::UserMgr *user_mgr = pg_proxy::UserMgr::get_instance();
        std::string db_name = "template1";

        // change password encryption
        _change_encryption(MD5_ENCRYPTION);

        // add user
        _add_user("aaa", "aaa_password");
        sleep(2 * _sleep_interval);
        pg_proxy::UserPtr user = user_mgr->get_user("aaa", db_name);
        ASSERT_NE(user, nullptr);
        std::string user_password = user->password();
        ASSERT_TRUE(user_password.starts_with("md5"));

        // remove user and verify access
        _remove_user("aaa");
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", db_name);
        ASSERT_EQ(user, nullptr);

        // change password encryption
        _change_encryption(SCRAM_ENCRYPTION);

        // add user
        _add_user("aaa", "aaa_password");
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", db_name);
        ASSERT_NE(user, nullptr);
        user_password = user->password();
        ASSERT_TRUE(user_password.starts_with("SCRAM-SHA-256"));

        // remove user and verify access
        _remove_user("aaa");
        sleep(2 * _sleep_interval);
        user = user_mgr->get_user("aaa", db_name);
        ASSERT_EQ(user, nullptr);
    }
}