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
     *
     * NOTE: once added the message itself doesn't track if it is a dependency
     * message; that is tracked here in the QueryStatus object.
     */
    struct QueryStatus {
        SessionMsgPtr msg;                 ///< parent message being processed
        int query_count = 0;               ///< number of queries
        int query_complete_count = 0;      ///< number of queries completed
        bool dependency = false;           ///< is this a dependency message

        /** Constructor */
        explicit QueryStatus(SessionMsgPtr msg, bool dependency = false)
            : msg(msg), dependency(dependency)
        {
            if (msg->data()->children.size() > 0) {
                query_count = msg->data()->children.size();
            } else {
                query_count = 1;
            }
        }

        /** Check if this query is complete */
        bool is_complete() const {
            return (query_complete_count >= query_count);
        }

        /** Check if this is a dependency message */
        bool is_dependency() const {
            return dependency;
        }

        /** Mark this query as completing a message */
        bool mark_message_complete() {
            query_complete_count++;
            return is_complete();
        }
    };
    using QueryStatusPtr = std::shared_ptr<QueryStatus>;

    /**
     * @brief Server session object.
     * This object represents a session with a remote database.
     * The database may be either the primary or a replica
     */
    class ServerSession : public Session {
    public:

        /**
         * @brief Represents a batch of messages to be processed together
         *
         * A batch contains messages that can be pipelined to the server.
         * The batch may contain dependency messages that must
         * complete before regular query messages are sent.
         *
         * We track two types of completion:
         * 1. Individual message completion (for client callbacks via _client_msg_response)
         *    - Tracked per QueryStatus via query_complete_count
         *    - Triggers client notification when message completes
         *
         * 2. ReadyForQuery completion (for batch phase transitions)
         *    - Tracked at batch level via ready_for_query counters
         *    - Triggers state transitions between dependency and query phases
         *
         * Processing phases:
         * 1. Dependency phase: All messages that contain dependencies are sent and completed
         *    - Dependencies never send responses to client
         *    - Wait for all dependency ReadyForQuery before proceeding
         *
         * 2. Query phase: All regular query messages are sent and completed
         *    - Send responses to client as each message completes
         *    - Wait for all query ReadyForQuery before batch completes
         */
        struct Batch {

            /** Constructor, add list of messages */
            explicit Batch(const std::vector<SessionMsgPtr>& messages) {
                for (const auto& msg : messages) {
                    if (msg->is_dependency_message()) {
                        DCHECK(msg->num_dependencies() > 0) <<
                                "Dependency message missing dependencies";
                        // iterate dependencies and add each as separate QueryStatus
                        for (const auto& dep_msg : msg->dependencies()) {
                            add_message(dep_msg, true);
                        }
                    } else {
                        add_message(msg);
                    }
                }
            }

            /**
             * @brief Queue of QueryStatus objects for messages in this batch
             *
             * Processed in FIFO order matching server response order.
             * PostgreSQL guarantees in-order processing and responses.
             *
             * When the query queue is empty, the batch is complete.
             * Each message may contain multiple queries; when all queries
             * are complete, the batch is considered complete.  The query_status
             * structure tracks individual message completions.
             */
            std::deque<QueryStatusPtr> query_status_queue;

            /**
             * @brief Check if all dependencies in this batch have completed
             * Assumes dependencies are processed first in the batch.
             * @return True if all dependency ReadyForQuery responses received
             */
            bool are_dependencies_complete() const {
                auto query_state = get_current_query_status();
                if (query_state && query_state->is_dependency()) {
                    return false;
                }
                return true;
            }

            /**
             * @brief Check if the entire batch is complete
             * @return True if both dependency and query phases are complete
             */
            bool is_complete() const {
                return (query_status_queue.empty());
            }

            /**
             * @brief Get the current QueryStatus being processed
             * @return Front of query_status_queue, or nullptr if empty
             */
            QueryStatusPtr get_current_query_status() const {
                if (query_status_queue.empty()) {
                    return nullptr;
                }
                return query_status_queue.front();
            }

            /**
             * @brief Remove the completed QueryStatus from front of queue
             * Called when all statements in a message have completed.
             * The next message in queue becomes the current message.
             */
            void remove_completed_query_status() {
                if (!query_status_queue.empty()) {
                    query_status_queue.pop_front();
                }
            }

            /**
             * @brief Get total number of messages in this batch
             * @return Size of query_status_queue
             */
            size_t get_total_message_count() const {
                return query_status_queue.size();
            }

            /**
             * @brief Check if batch is empty
             * @return True if no messages in batch
             */
            bool is_empty() const {
                return query_status_queue.empty();
            }

            /**
             * @brief Mark current message's query/dependency as complete
             * Increments the appropriate completion counter in QueryStatus.
             * If the message is fully complete, it is removed from the queue.
             * @return True if the current message is fully complete and removed
             */
            bool mark_message_complete() {
                auto query_status = get_current_query_status();
                if (!query_status) {
                    return false;
                }

                query_status->mark_message_complete();

                // If query is complete, remove from queue
                if (query_status->is_complete()) {
                    remove_completed_query_status();
                    return true;
                }

                return false;
            }

            /**
             * @brief Add a message to this batch
             * Creates a QueryStatus for the message and updates counters.
             * @param message SessionMsgPtr to add
             * @param dependency true if this is a dependency message
             */
            void add_message(SessionMsgPtr message, bool dependency = false) {
                QueryStatusPtr query_status = std::make_shared<QueryStatus>(message, dependency);
                query_status_queue.push_back(query_status);
            }
        };
        using BatchPtr = std::shared_ptr<Batch>;

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
                client_session->server_shutdown(shared_from_this());
            }
            unpin_client_session();
            _defer_shadow_shutdown = false;
            _stmts.clear();
            _batch_queue.reset();
            _current_batch.reset();
            _seq_id = 0;
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

        /** Forward message to server out-of-order bypasses batch mechanism */
        void forward_msg(SessionMsgPtr msg);

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

    private:

        bool _is_pinned = false;

        std::weak_ptr<ClientSession> _client_session; ///< client session

        SessionMsgQueue<SessionMsgPtr> _batch_queue; ///< queue for client msg batches

        std::set<std::string> _stmts;        ///< completed prepared statement ids

        std::string _db_prefix;              ///< database name prefix (for testing)

        uint64_t _seq_id = 0;                ///< sequence id from client session

        ServerAuthorizationPtr _auth;        ///< authorization object

        bool _defer_shadow_shutdown = false; ///< defer shutdown until all messages processed

        BatchPtr _current_batch;             ///< current batch being processed

        /**
         * @brief Process next batch of messages from _batch_queue
         *
         * Called when:
         * - Current batch is complete
         * - Server session is ready for more work
         * - There are batches waiting in _batch_queue
         *
         * Processing steps:
         * 1. Load next batch from _batch_queue
         * 2. Create Batch structure with QueryStatus queue and counters
         * 3. Determine if batch has dependencies
         * 4. Set initial state (DEPENDENCIES or QUERY)
         * 5. Send appropriate messages (all dependencies or all queries)
         *
         * Dependencies and queries are pipelined within their respective phases.
         * All dependencies must complete before any queries are sent.
         */
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

        /** Send simple query */
        void _send_simple_query(const std::string &query, uint64_t seq_id);

        /** Handle replies from server */
        void _handle_message_from_server();

        /** Handle error code 'E' -- returns error code */
        std::string _decode_error_buffer(BufferPtr buffer, uint64_t seq_id);

        /**
         * @brief Handle query error response from server
         *
         * Called when server sends an ErrorResponse message during query processing.
         * This indicates that a query statement failed.
         *
         * For simple protocol:
         * - Error terminates the entire query message
         * - Server sends ErrorResponse followed by ReadyForQuery
         * - Mark entire message as failed
         *
         * For extended protocol:
         * - Error occurs during Parse/Bind/Execute
         * - Server requires Sync message to recover
         * - Enter EXTENDED_ERROR state
         * - Wait for Sync to be processed
         *
         * Processing:
         * 1. Get current QueryStatus from front of batch queue
         * 2. Log error information
         * 3. Determine protocol type (simple vs extended)
         * 4. Handle error appropriately:
         *    - Simple: Mark message as failed, notify client
         *    - Extended: Enter error recovery state
         * 5. Remove failed QueryStatus from queue
         */
        void _handle_query_error();

        /**
         * @brief Handle ReadyForQuery during extended error recovery
         *
         * Called when ReadyForQuery is received while in EXTENDED_ERROR state.
         * This occurs after an extended protocol error and subsequent Sync message.
         *
         * The server sends ReadyForQuery after processing Sync, which clears
         * the error state and allows normal processing to resume.
         *
         * Processing:
         * 1. Verify we're in EXTENDED_ERROR state
         * 2. Log error recovery completion
         * 3. Send ReadyForQuery to client (client needs to know error is cleared)
         * 4. Transition back to QUERY state
         * 5. Continue processing remaining messages in batch
         *
         * @param transaction_status Transaction status character from ReadyForQuery (I/T/E)
         *                          'E'` indicates failed transaction (most common after error)
         *                          'I' indicates idle (if error was outside transaction)
         *                          'T' indicates transaction (unlikely after error)
         */
        void _handle_extended_error_ready_for_query(char transaction_status);

        /**
         * @brief Handle msg response (CommandComplete, etc.)
         *
         * Called when server sends a completion message for a query statement.
         * This indicates that one statement within a message has completed.
         * A message may contain multiple statements (e.g., "SELECT 1; SELECT 2;").
         *
         * Processing:
         * 1. Get current QueryStatus from front of batch queue
         * 2. Increment query_complete_count
         * 3. If all statements in message complete, call _client_msg_response()
         * 4. Remove completed QueryStatus from queue
         *
         * Note: This is separate from ReadyForQuery handling.
         * ReadyForQuery indicates server is ready for next message.
         * CommandComplete indicates a statement within current message is done.
         */
        void _handle_msg_response();

        /**
         * @brief Route ReadyForQuery to appropriate handler based on current state
         *
         * ReadyForQuery can arrive during different states:
         * - DEPENDENCIES: Count toward dependency completion
         * - QUERY: Count toward query completion, send to client
         * - EXTENDED_ERROR: Special error recovery handling
         *
         * This function routes to the appropriate handler based on current state.
         *
         * @param transaction_status Transaction status character from ReadyForQuery (I/T/E)
         *                          'I' = idle (no transaction)
         *                          'T' = in transaction block
         *                          'E' = in failed transaction block
         */
        void _handle_ready_for_query_response(char xact_status);

        /**
         * @brief Handle ReadyForQuery response during dependency phase
         *
         * ReadyForQuery indicates the server has completed processing and is ready
         * for the next message. During dependency phase, we count these responses
         * to determine when all dependencies have completed.
         *
         * Processing:
         * 1. Increment dependency ReadyForQuery counter in batch
         * 2. Do NOT send ReadyForQuery to client (dependencies never respond)
         * 3. If all dependency ReadyForQuery received, transition to query phase
         *
         * Note: One ReadyForQuery may correspond to multiple dependency messages
         * if they are extended protocol (Parse/Bind/Sync).
         *
         * @param transaction_status Transaction status character from ReadyForQuery (I/T/E)
         */
        void _handle_dependency_ready_for_query(char transaction_status);

        /**
         * @brief Handle ReadyForQuery response.
         *
         * ReadyForQuery indicates the server has completed processing and is ready
         * for the next message. During query phase, we send this to the client and
         * count responses to determine when all queries have completed.
         * During dependency phase, we count responses to determine when all dependencies
         * have completed and we can transition to query phase.
         *
         * Processing:
         * 1. Increment query ReadyForQuery counter in batch
         * 2. Send ReadyForQuery to client (client session callback: server_ready_msg)
         * 3. If all query ReadyForQuery received, batch is complete
         * 4. Clean up batch and process next batch if available
         *
         * Note: One ReadyForQuery may correspond to multiple query messages
         * if they are extended protocol (Parse/Bind/Execute/Sync).
         *
         * @param transaction_status Transaction status character from ReadyForQuery (I/T/E)
         */
        void _handle_query_ready_for_query(char transaction_status);

        /** Process response from server in reset session state */
        void _handle_reset_session_message();

        /** Helper to read data and drop it */
        void _read_and_drop_message(int msg_length);

        /** Encode status parameters using simple query, return true if sent */
        bool _send_status_query(const std::unordered_map<std::string, std::string> &parameters);

        /**
         * @brief Send all messages in current batch (pipelined)
         *
         * If first message is a replay dependency, send dependencies first.
         * Dependency messages restore session state.
         * They must complete before any regular queries are sent.
         *
         * All messages are sent to the server without waiting for responses
         * (pipelined). This includes both simple query and extended protocol messages.
         *
         * For Query msgs: We send responses to client as each query completes.
         * For Dependency msgs: We wait for all ReadyForQuery responses before transitioning to query phase.
         */
        void _send_all_messages();

        /**
         * @brief Send message to server
         * @param query_status query status object
         */
        void _send_msg(QueryStatusPtr query_status);

        /**
         * @brief Send message completion notification to client session
         *
         * Called when all statements in a message have completed.
         * This is separate from ReadyForQuery - it indicates message completion,
         * not server readiness.
         *
         * Client session callback: server_msg_response()
         *
         * For dependencies, this function should not be called (dependencies
         * never send responses to client).
         *
         * @param msg Session message that completed
         * @param success True if message completed successfully
         */
        void _client_msg_response(SessionMsgPtr msg, bool success);

        /**
         * @brief Helper to release the session
         * @param deallocate if true it will deallocate the session and not add to the pool
         */
        void _release_session(bool deallocate);
    };
    using ServerSessionPtr = std::shared_ptr<ServerSession>;
    using ServerSessionWeakPtr = std::weak_ptr<ServerSession>;
} // namespace springtail::pg_proxy
