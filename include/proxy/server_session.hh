#pragma once

#include <proxy/session.hh>
#include <proxy/client_session.hh>
#include <proxy/session_msg.hh>

namespace springtail::pg_proxy {

    class DatabaseInstance;
    using DatabaseInstancePtr = std::shared_ptr<DatabaseInstance>;

    /**
     * @brief Internal state for a query that is being processed.
     * A query may have multiple dependencies, and may have multiple
     * simple queries. This object tracks the completion state of the query.
     */
    struct QueryStatus {
        int query_count = 0;               ///< number of (simple) queries
        int query_complete_count = 0;      ///< number of (simple) queries completed
        int dependency_count = 0;          ///< number of dependencies
        int dependency_complete_count = 0; ///< number of dependencies completed
        bool simple_query_dependency = false; ///< at least one simple query dependency
        SessionMsgPtr msg;                 ///< parent message being processed

        QueryStatus(SessionMsgPtr msg)
            : msg(msg)
        {}
    };
    using QueryStatusPtr = std::shared_ptr<QueryStatus>;

    /**
     * @brief Server session object.
     * This object represents a session with a remote database.
     * The database may be either the primary or a replica
     */
    class ServerSession : public Session {
    public:
        ServerSession(const ServerSession&) = delete;
        ServerSession& operator=(const ServerSession&) = delete;

        ServerSession(ProxyConnectionPtr connection,
                      UserPtr user,
                      std::string database,
                      std::string prefix,
                      DatabaseInstancePtr instance,
                      const std::unordered_map<std::string, std::string> &parameters,
                      Session::Type type=Type::PRIMARY);

        /** For test purposes */
        ServerSession(Session::Type type,
                      uint64_t id,
                      uint64_t db_id,
                      const std::string &database,
                      const std::string &username)
            : Session(type, id, db_id, database, username)
        {}

        ~ServerSession();

        /** Entry point from server */
        void run(std::set<int> &fds) override;

        /** Session name */
        std::string name() const override {
            return fmt::format("Server[{}]", _id);
        }

        /** Shutdown session called from Session::_handle_error() */
        void shutdown_session() override;

        /**
         * @brief Pin this session to a client session
         * @return true if pinned
         */
        bool is_pinned() const {
            return _is_pinned;
        }

        /**
         * @brief Pin this session to a client session
         * @param client_session client session to pin
         */
        void pin_client_session(std::shared_ptr<ClientSession> client_session) {
            // internally the session.cc code uses the associated session
            set_associated_session(client_session);
            _client_session = client_session;
            _is_pinned = true;
        }

        /**
         * @brief Unpin the client session and disassociate it
         */
        void unpin_client_session() {
            if (!_is_pinned) {
                return;
            }
            clear_associated_session();
            _client_session.reset();
            _is_pinned = false;
        }

        /**
         * @brief Get the client session object
         * @return SessionPtr
         */
        ClientSessionPtr get_client_session() {
            ClientSessionPtr client = _client_session.lock();
            if (client == nullptr) {
                client = std::dynamic_pointer_cast<ClientSession>(get_associated_session());
            }
            return client;
        }

        /**
         * @brief Get shared pointer to this server session
         * @return std::shared_ptr<ServerSession> shared pointer to this server session
         */
        std::shared_ptr<ServerSession> shared_from_this() {
            return std::static_pointer_cast<ServerSession>(Session::shared_from_this());
        }

        /**
         * @brief Reset the session, override base session (and call it)
         */
        void reset_session() override {
            auto client_session = get_client_session();
            if (client_session != nullptr) {
                client_session->server_shutdown(shared_from_this(), _fatal_error);
            }
            unpin_client_session();
            _defer_shadow_shutdown = false;
            _stmts.clear();
            while (!_pending_queue.empty()) {
                _pending_queue.pop();
            }
            _batch_queue.reset();
            // reset base session - clears associated session, etc
            Session::reset_session();
        }

        /**
         * @brief Check if the session's startup params match the client's
         * @param parameters client parameters
         * @return true if match
         * @return false if not a match
         */
        bool check_startup_params(const std::unordered_map<std::string, std::string> &parameters);

        /**
         * @brief Set the session to ready state after being reset
         */
        void set_ready_reset_done();

        /**
         * @brief Called from client session to start authorization
         * @param seq_id sequence id
         * @param parameters startup parameters
         */
        void startup(uint64_t seq_id);

        /**
         * @brief Entry point for processing connection data
         * @param seq_id sequence id
         */
        void process_connection(uint64_t seq_id);

        /**
         * @brief Queue message from client session to current batch
         * @param msg_batch messages to process
         */
        void queue_msg_batch(std::deque<SessionMsgPtr> msg_batch);

        /** Process single message -- most messages should go through queue_msg_batch() */
        void process_msg(SessionMsgPtr msg);

        /**
         * @brief Process shutdown from client session
         * @param seq_id sequence id
         */
        void process_shutdown(uint64_t seq_id);

        /**
         * @brief Reset a session with new startup params (issued via simple query)
         * @param seq_id sequence id
         * @param parameters startup parameters from client session
         */
        void startup_reset_session(uint64_t seq_id, const std::unordered_map<std::string, std::string> &parameters);

        /** factory to create session */
        static std::shared_ptr<ServerSession>
        create(UserPtr user, const std::string &database,
               const std::string &prefix,
               DatabaseInstancePtr instance,
               Session::Type type,
               const std::unordered_map<std::string, std::string> &parameters);

        /** send cancel request to Postgres */
        void send_cancel();

        /** transfer batch queue from session to the current session */
        void transfer_batch_queue(ServerSessionPtr session);

        /** set user valid flag to false */
        void invalidate_db_user() { _is_user_valid = false; }

        /** return the value of user valid flag */
        bool is_user_valid() { return _is_user_valid; }

        /**
         * @brief Return JSON representation of the server session. It overrides base
         *      class function with the same name and prints additional information
         *      pertaining to server session objecct.
         *
         * @return nlohmann::json
         */
        nlohmann::json
        to_json() const override
        {
            nlohmann::json j = Session::to_json();
            j.update(nlohmann::json::object({
                {"is_user_valid", _is_user_valid.load()}
            }));
            return j;
        }

    private:

        bool _is_pinned = false;

        std::weak_ptr<ClientSession> _client_session; ///< client session

        SessionMsgQueue<SessionMsgPtr> _batch_queue; ///< queue for client msg batches

        std::queue<QueryStatusPtr> _pending_queue;   ///< queue of pending (inflight) messages

        std::set<std::string> _stmts;        ///< completed prepared statement ids

        std::string _db_prefix;              ///< database name prefix (for testing)

        uint64_t _seq_id = 0;                ///< sequence id from client session

        ServerAuthorizationPtr _auth;        ///< authorization object

        bool _defer_shadow_shutdown = false; ///< defer shutdown until all messages processed

        bool _fatal_error = false;           ///< flag indicating fatal error

        std::atomic<bool> _is_user_valid{true};     ///< atomic flag indicating if the user of the session is valid

        /** Process next batch of processing messages from _batch_queue */
        void _process_next_batch();

        /** Send shutdown to server */
        void _send_shutdown();

        /** Send reset query cmds to reset session */
        void _send_reset();

        /**
         * @brief Send required statements to fulfil dependency
         * @param dependency dependency to fulfil
         */
        void _send_dependency(const QueryStmtPtr dependency, uint64_t seq_id);

        void _send_server_msg(QueryStatusPtr query_status);

        /** Send simple query */
        void _send_simple_query(const std::string &query, uint64_t seq_id);

        /** Handle replies from server */
        void _handle_message_from_server();

        /** Handle error code 'E' -- returns error code */
        std::string _decode_error_buffer(BufferPtr buffer, uint64_t seq_id);

        /** Handle forwarded message, first replay dependencies */
        void _handle_msg_to_server(SessionMsgPtr msg);

        /** Handle dependency response; true if error */
        void _handle_dependency_response(bool error);

        /** Handle query response */
        void _handle_query_response();

        /** Handle query error */
        void _handle_query_error();

        /** Handle ready for query message from server */
        void _handle_ready_for_query_response(char xact_status);

        /** Process response from server in reset session state */
        void _handle_reset_session_message();

        /** Helper to read data and drop it */
        void _read_and_drop_message(int msg_length);

        /** Encode status parameters using simple query, return true if sent */
        bool _send_status_query(const std::unordered_map<std::string, std::string> &parameters);

        /** Get the type of the head of the pending queue */
        QueryStmt::Type _get_pending_query_type();

        /** Send message response to the client session */
        void _client_msg_response(SessionMsgPtr msg, bool success);

        /**
         * @brief Helper to release the session
         * @param deallocate if true it will deallocate the session and not add to the pool
         */
        bool _release_session(bool deallocate);

        /**
         * @brief Perform server session cleanup.
         *
         */
        void _session_cleanup();
    };
    using ServerSessionPtr = std::shared_ptr<ServerSession>;
    using ServerSessionWeakPtr = std::weak_ptr<ServerSession>;
} // namespace springtail::pg_proxy
