#pragma once

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
#include <proxy/request_handler.hh>
#include <proxy/session.hh>
#include <proxy/user_mgr.hh>

#include <proxy/buffer_pool.hh>
#include <proxy/database.hh>

namespace springtail {
    class ProxyServer : public std::enable_shared_from_this<ProxyServer> {
    public:
        ProxyServer(int port,
                    int thread_pool_size,
                    const std::filesystem::path &cert_file,
                    const std::filesystem::path &key_file,
                    bool enable_ssl=true);

        /** Start server main loop */
        void run();

        /** Signal server main loop to reset poll fd set */
        void signal(ProxyConnectionPtr connection);

        /** Register a new session, add to <socket, session> to _sessions map*/
        void register_session(SessionPtr session);

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

        /** Add a database */
        void add_replicated_database(const std::string &dbname) {
            std::unique_lock lock(_db_mutex);
            _replicated_databases.insert(dbname);
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
        DatabaseInstancePtr get_replica_instance(const std::string &dbname, const std::string &username) {
            return _replica_set.get_replica(dbname, username);
        }

        /** Set primary db instance */
        void set_primary(DatabaseInstancePtr instance) {
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

    private:
        int _socket;   ///< server socket
        int _pipe[2];  ///< pipe for interrupting poll loop; [0] - read; [1] - write

        ProxyRequestHandlerPtr _request_handler;

        UserMgrPtr _user_mgr;                ///< user manager object

        ThreadPool<Session> _thread_pool;    ///< thread pool for handling incoming session data

        std::mutex _waiting_sessions_mutex;  ///< mutex for _waiting_sessions set and _sessions map
        std::set<int> _waiting_sessions;     ///< set of connection sockets waiting for read data
        std::map<int, SessionPtr> _sessions; ///< map of connection socket to session object

        SSL_CTX *_ssl_ctx_server = nullptr;  ///< SSL context for server
        SSL_CTX *_ssl_ctx_client = nullptr;  ///< SSL context for client

        bool _enable_ssl;                    ///< true if SSL is enabled

        DatabasePrimarySet _primary_database; ///< set of primary databases
        DatabaseReplicaSet _replica_set;      ///< set of replica databases

        std::shared_mutex _db_mutex;
        std::set<std::string> _replicated_databases; ///< list of authorized databases

        /** Accept handler -- called from poll loop */
        void _do_accept();

        /** Setup and configure SSL context; pass in certificate and private key */
        SSL_CTX *_setup_SSL_context(const std::filesystem::path &cert_file={},
                                    const std::filesystem::path &key_file={});
    };
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

}