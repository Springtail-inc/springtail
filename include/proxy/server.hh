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

namespace springtail::pg_proxy {

    class ProxyServer : public std::enable_shared_from_this<ProxyServer> {
    public:
        static constexpr uint32_t USER_MGR_SLEEP_INTERVAL_SECS = 5;

        enum MODE : int8_t {
            NORMAL=0,   ///< normal mode, read-write splitting
            PRIMARY=1,  ///< primary mode, all traffic to primary
            SHADOW=2    ///< shadow mode, replica shadows primary and logs
        };

        /**
         * @brief Construct a new Proxy Server object
         * The server handles the poll loop and accepts new connections.
         * It dispatches readable sockets into the thread pool
         * @param port - port to listen for connections on
         * @param thread_pool_size - number of threads in the thread pool
         * @param cert_file - path to the server certificate file
         * @param key_file - path to the server key file
         * @param mode - mode of the server
         * @param enable_ssl - enable SSL
         * @param logger - logger object for shadow mode
         */
        ProxyServer(int port,
                    int thread_pool_size,
                    const std::filesystem::path &cert_file,
                    const std::filesystem::path &key_file,
                    MODE mode=MODE::NORMAL,
                    bool enable_ssl=false,
                    LoggerPtr shadow_logger=nullptr);

        /** Start server main loop */
        void run();

        /** Cleanup server resources */
        void cleanup();

        /** Signal server main loop to reset poll fd set */
        void signal(ProxyConnectionPtr connection);

        /** Register a new session, add to <socket, session> to _sessions map*/
        void register_session(SessionPtr session,
                              bool waiting_session_insert=false);

        /** Cleanup a session, remove from _sessions_map, remove from poll fd set */
        void shutdown_session(SessionPtr session);

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
        bool is_ssl_enabled() const {
            return _enable_ssl;
        }

        /** Get the logger object */
        LoggerPtr get_logger() const {
            return _logger;
        }

        /** Shutdown server */
        void shutdown();

        /** Get the proxy id */
        uint32_t id() const {
            return _id;
        }

        /** Get the server mode */
        MODE mode() const {
            return _mode;
        }

        /** Set the global log level */
        void set_log_level(int loglevel);

    private:
        int _socket;   ///< server socket
        int _pipe[2];  ///< pipe for interrupting poll loop; [0] - read; [1] - write

        uint32_t _id;  ///< unique id for this proxy server
        ThreadPool<Session> _thread_pool;    ///< thread pool for handling incoming session data

        std::mutex _waiting_sessions_mutex;  ///< mutex for _waiting_sessions set and _sessions map
        std::set<int> _waiting_sessions;     ///< set of connection sockets waiting for read data
        std::map<int, SessionPtr> _sessions; ///< map of connection socket to session object

        SSL_CTX *_ssl_ctx_server = nullptr;  ///< SSL context for server
        SSL_CTX *_ssl_ctx_client = nullptr;  ///< SSL context for client

        bool _enable_ssl = false;            ///< true if SSL is enabled

        MODE _mode;                          ///< server mode
        std::atomic<bool> _shutdown = false; ///< true if server is shutting down

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
    };
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

} // namespace springtail::pg_proxy
