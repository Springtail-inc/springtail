#pragma once

#include <memory>
#include <string>
#include <map>
#include <set>
#include <filesystem>

#include <openssl/ssl.h>

#include <common/thread_pool.hh>

#include <proxy/connection.hh>
#include <proxy/request_handler.hh>
#include <proxy/session.hh>
#include <proxy/user_mgr.hh>

#include <event2/event.h>


namespace springtail {
    class ProxyServer : public std::enable_shared_from_this<ProxyServer> {
    public:
        ProxyServer(const std::string &address,
                    int port,
                    int thread_pool_size,
                    const std::filesystem::path &cert_file,
                    const std::filesystem::path &key_file);

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
        void add_user(const std::string &username, const std::string &database) {
            _user_mgr->add_user(username, database);
        }

        /** Add a user to the user manager */
        void add_user(const std::string &username, const std::string &database,
                      const std::string &password, uint32_t salt=0) {
            _user_mgr->add_user(username, database, password, salt);
        }

        /** Add a database hostname, port to user mgr */
        void add_database(const std::string &dbname, const std::string &hostname, int port) {
            _user_mgr->add_database(dbname, hostname, port);
        }

        SSL *SSL_new() {
            // think this is thread safe but hard to know 100%
            return ::SSL_new(_ssl_ctx);
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

        SSL_CTX *_ssl_ctx;                    ///< SSL context

        void _do_accept();                   ///< accept new connection handler
    };
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

}