#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <map>
#include <set>
#include <filesystem>
#include <mutex>
#include <shared_mutex>

#include <openssl/ssl.h>

#include <common/thread_pool.hh>

#include <proxy/connection.hh>
#include <proxy/session.hh>
#include <proxy/user_mgr.hh>

#include <proxy/buffer_pool.hh>
#include <proxy/database.hh>
#include <proxy/logger.hh>

#include <redis/db_state_change.hh>
#include <redis/pubsub_thread.hh>

namespace springtail::pg_proxy {

    class ProxyServer : public std::enable_shared_from_this<ProxyServer> {
    public:
        ProxyServer(int port,
                    int thread_pool_size,
                    const std::filesystem::path &cert_file,
                    const std::filesystem::path &key_file,
                    bool shadow_mode=false,
                    bool enable_ssl=false,
                    LoggerPtr logger=nullptr);

        /** Start server main loop */
        void run();

        /** Signal server main loop to reset poll fd set */
        void signal(ProxyConnectionPtr connection);

        /** Register a new session, add to <socket, session> to _sessions map*/
        void register_session(SessionPtr session,
                              bool waiting_session_insert=false);

        /** Cleanup a session, remove from _sessions_map, remove from poll fd set */
        void shutdown_session(SessionPtr session);

        /** Get the user mgr object */
        UserMgrPtr get_user_mgr() {
            return _user_mgr;
        }

        /** Add a user to the user manager -- trust authentication no password */
        void add_user(const std::string &username) {
            _user_mgr->add_user(username);
        }

        /** Add a user to the user manager */
        void add_user(const std::string &username,
                      const std::string &password, uint32_t salt=0) {
            _user_mgr->add_user(username, password, salt);
        }

        /**
         * @brief Add replicated database id and name the collection of replicated databases
         *          and setup initial state per database
         *
         * @param db_id - database id
         * @param dbname - database name
         */
        void add_replicated_database(const uint64_t db_id, const std::string &dbname) {
            std::unique_lock lock(_db_mutex);
            _replicated_databases[dbname] = db_id;

            // TODO: I think this is no longer needed as the real initialization will happen somewhere else
            std::unique_lock db_state_lock(_db_state_mutex);
            _replicated_database_states[db_id] = redis::db_state_change::DB_STATE_INITIALIZE;
        }

        /**
         * @brief Get database id for given database name
         *
         * @param dbname - database name
         * @return uint64_t - database id
         */
        uint64_t get_database_id(const std::string &dbname) {
            std::shared_lock lock(_db_mutex);
            return _replicated_databases[dbname];
        }

        /**
         * @brief Get the state of the given database
         *
         * @param db_id - database id
         * @return redis::db_state_change::DBState
         */
        redis::db_state_change::DBState get_database_state(const uint64_t db_id) {
            std::shared_lock lock(_db_state_mutex);
            return _replicated_database_states[db_id];
        }

        /** Check if a database is replicated */
        bool is_database_replicated(const std::string &dbname) {
            std::shared_lock lock(_db_mutex);
            return _replicated_databases.contains(dbname);
        }

        /** Allocate SSL struct for new connection */
        SSL *SSL_new(bool is_server) {
            // think this is thread safe but hard to know 100%
            if (is_server) {
                return ::SSL_new(_ssl_ctx_server);
            } else {
                return ::SSL_new(_ssl_ctx_client);
            }
        }

        /** Is ssl enabled globally? */
        bool is_ssl_enabled() {
            return _enable_ssl;
        }

        /** Get primary database instance */
        DatabaseInstancePtr get_primary_instance() {
            return _primary_database.primary();
        }

        /** Get replica database instance  -- use username/dbname as a hint */
        DatabaseInstancePtr get_replica_instance(const uint64_t db_id, const std::string &username) {
            return _replica_set.get_replica(db_id, username);
        }

        /** Set primary db instance */
        void set_primary(uint64_t instance_id, DatabaseInstancePtr instance) {
            _db_instance_id = instance_id;
            _primary_database.set_primary(instance);
        }

        /** Set the secondary db instance */
        void set_standby(DatabaseInstancePtr instance) {
            _primary_database.set_standby(instance);
        }

        /** Add replica instance */
        void add_replica(DatabaseInstancePtr instance) {
            _replica_set.add_replica(instance);
        }

        /** Get the logger object */
        LoggerPtr get_logger() {
            return _logger;
        }

        /** Shutdown server */
        void shutdown();

        /** Get the proxy id */
        uint32_t id() {
            return _id;
        }

        bool is_table_replicated(uint64_t db_id, const std::string &schema, const std::string &table) {
            return _schema_tables.has_item(db_id, schema, table);

        }
    private:
        int _socket;   ///< server socket
        int _pipe[2];  ///< pipe for interrupting poll loop; [0] - read; [1] - write

        uint32_t _id;  ///< unique id for this proxy server
        uint64_t _db_instance_id;           ///< primary database instance id

        UserMgrPtr _user_mgr;                ///< user manager object

        PubSubThread _config_sub_thread;    ///< pubsub thread for redis config database
        PubSubThread _data_sub_thread;      ///< pubsub thread for redis data database
        ThreadPool<Session> _thread_pool;    ///< thread pool for handling incoming session data

        std::mutex _waiting_sessions_mutex;  ///< mutex for _waiting_sessions set and _sessions map
        std::set<int> _waiting_sessions;     ///< set of connection sockets waiting for read data
        std::map<int, SessionPtr> _sessions; ///< map of connection socket to session object

        SSL_CTX *_ssl_ctx_server = nullptr;  ///< SSL context for server
        SSL_CTX *_ssl_ctx_client = nullptr;  ///< SSL context for client

        bool _enable_ssl = false;            ///< true if SSL is enabled

        DatabasePrimarySet _primary_database; ///< set of primary databases
        DatabaseReplicaSet _replica_set;      ///< set of replica databases

        std::shared_mutex _db_mutex;          ///< shared mutex for read/write access to the replicated databases map
        std::map<std::string, uint64_t> _replicated_databases; ///< list of authorized databases with associated ids

        std::shared_mutex _db_state_mutex;   ///< shared mutex for read/write access to database state
        std::map<uint64_t, redis::db_state_change::DBState> _replicated_database_states; ///< list of authorized database ids

        // std::shared_mutex _schema_tables_mutex;  ///< shared mutex lock for schema tables storage
        DatabaseSchemaTableStore _schema_tables; ///< storage of schema and table info per database

        bool _shadow_mode = false; ///< shadow mode flag; if true, replca shadows primary
        std::atomic<bool> _shutdown = false;    ///< true if server is shutting down

        LoggerPtr _logger;         ///< logger object (may be null)

        /** Accept handler -- called from poll loop */
        void _do_accept();

        /** Setup and configure SSL context; pass in certificate and private key */
        SSL_CTX *_setup_SSL_context(const std::filesystem::path &cert_file={},
                                    const std::filesystem::path &key_file={});

        /** Log new connection */
        void _log_connect(SessionPtr session);

        /** Log disconnect */
        void _log_disconnect(SessionPtr session);

        /**
         * @brief Database state change handling
         *
         * @param msg - message
         */
        void _handle_db_state_change(const std::string &msg);

        /**
         * @brief Database schema and table change handling
         *
         * @param msg - message
         */
        void _handle_db_table_change(const std::string &msg);

        /**
         * @brief Initialize pubsub thread for database state change
         *
         */
        void _init_db_states_subscriber();

        /**
         * @brief Initialize pubsub thread for database tables subscriber
         *
         */
        void _init_db_tables_subscriber();

    };
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

} // namespace springtail::pg_proxy
