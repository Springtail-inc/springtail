#pragma once

#include <atomic>
#include <memory>
#include <map>
#include <set>
#include <filesystem>
#include <mutex>
#include <unordered_map>

#include <openssl/ssl.h>

#include <common/thread_pool.hh>
#include <common/init.hh>

#include <proxy/connection.hh>
#include <proxy/session.hh>

#include <proxy/buffer_pool.hh>
#include <proxy/logger.hh>

namespace springtail::pg_proxy {

    class ProxyServer : public Singleton<ProxyServer>
    {
        friend class Singleton<ProxyServer>;
    public:
        enum class MODE : int8_t {
            NORMAL=0,   ///< normal mode, read-write splitting
            PRIMARY=1,  ///< primary mode, all traffic to primary
            SHADOW=2    ///< shadow mode, replica shadows primary and logs
        };

        /**
         * @brief Initialize a new Proxy Server object
         * The server handles the poll loop and accepts new connections.
         * It dispatches readable sockets into the thread pool
         * @param port - port to listen for connections on
         * @param thread_pool_size - number of threads in the thread pool
         * @param cert_file - path to the server certificate file
         * @param key_file - path to the server key file
         * @param keep_alive_port - port for keepalive connections, 0 disable
         * @param mode - mode of the server
         * @param enable_ssl - enable SSL
         * @param logger - logger object for shadow mode
         */
        void init(int port,
                  int thread_pool_size,
                  const std::filesystem::path &cert_file,
                  const std::filesystem::path &key_file,
                  int keep_alive_port=0,
                  MODE mode=MODE::NORMAL,
                  bool enable_ssl=false,
                  LoggerPtr shadow_logger=nullptr);

        /** Start server main loop */
        void run();

        /** Signal server main loop to reset poll fd set */
        void signal(SessionPtr session);

        /**
         * @brief Register a new session, add to <socket, session> to _sessions map
         * Remove old session association if it exists
         * @param new_session session to add
         * @param old_session session to replace
         * @param socket socket for the session
         * @param waiting_session_insert true if session is being added to _waiting_sessions
         */
        void register_session(SessionPtr new_session,
                              SessionPtr old_session,
                              int socket,
                              bool waiting_session_insert=false);

        /** Cleanup a session, remove from _sessions_map, remove all associated sockets */
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

        /** Get the proxy id */
        uint32_t id() const {
            return _id;
        }

        /** Get the server mode */
        MODE mode() const {
            return _mode;
        }

        /** Log new connection */
        void log_connect(SessionPtr session);

        /** Log disconnect */
        void log_disconnect(SessionPtr session);

        /** Notify server to shutdown */
        void notify_shutdown()
        {
            _shutdown = true;
            _wake_event_loop();
        }

        /** Notify server of pending message for session, via its socket */
        void notify(int socket);

        /** Start the proxy server singleton */
        static void start();


    protected:
        ProxyServer() : Singleton<ProxyServer>(ServiceId::ProxyServerId) {}
        virtual ~ProxyServer() override = default;

        /** Shutdown server */
        void _internal_shutdown() override;

    private:
        int _socket;   ///< server socket
        int _efd;      ///< eventfd for signaling

        uint32_t _id;  ///< unique id for this proxy server
        std::shared_ptr<ThreadPool<Session>> _thread_pool;    ///< thread pool for handling incoming session data
        std::thread _keep_alive_thread;  ///< thread for handling keepalive connections

        std::mutex _waiting_sessions_mutex;   ///< mutex for _waiting_sessions set and _sessions map
        std::set<int> _waiting_sessions;      ///< set of connection sockets waiting for read data
        std::map<int, SessionPtr> _sessions;  ///< map of connection socket to session object

        std::set<int> _notification_queue;    ///< set of sockets with notifications to process
        bool _notification_pending = false;   ///< true if notification is pending
        std::mutex _notification_mutex;       ///< mutex for _notification_queue

        /** map of session to connection socket */
        std::unordered_map<SessionPtr, std::vector<int>, Session::SessionHash, Session::SessionEqual> _session_sockets;

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

        /** Internal shutdown session -- helper, assumes lock is held */
        void _internal_shutdown_session(SessionPtr session, std::unique_lock<std::mutex> &lock);

        /** Wake up intternal event loop to reprocess waiting session */
        void _wake_event_loop();

        /** Start keep alive thread */
        void _start_keep_alive(int port);

        /** Helper to add a runnable fd, and remove its associated fds from the waiting sessions */
        void _add_runnable_fd(int fd, std::set<SessionPtr, Session::SessionComparator> &runnable_fds);

        /** Main server thread */
        std::thread _proxy_thread;
    };
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

} // namespace springtail::pg_proxy
