#pragma once

#include <shared_mutex>

#pragma push_macro("DELETE")
#include <httplib.h>
#pragma pop_macro("DELETE")

#include <nlohmann/json.hpp>

#include <common/exception.hh>
#include <common/logging.hh>
#include <common/singleton.hh>

namespace springtail {

    class HttpError : public Error {
    public:
        explicit HttpError(const std::string &msg, uint32_t error_code = 400) :
            Error(msg), _error_code(error_code) {}

        uint32_t get_error_code() { return _error_code; }
    private:
        uint32_t _error_code;
    };

    /**
     * @brief This class provides access to an embeded HTTP server.
     *
     */
    class AdminServer : public Singleton<AdminServer> {
        friend class Singleton<AdminServer>;
    public:
        /**
         * @brief GET request handler type
         *
         */
        using GetHandler = std::function<void(const std::string &path, const httplib::Params &params, nlohmann::json &json_response)>;

        /**
         * @brief POST request handler type
         *
         */
        using PostHandler = std::function<void(const std::string &path, const httplib::Params &params, const std::string &body, nlohmann::json &json_response)>;

        /**
         * @brief Redigster a handler for GET path
         *
         * @param path - path
         * @param handler - handler function
         */
        void
        register_get_route(const std::string& path, GetHandler &&handler)
        {
            std::unique_lock lock(_mutex);
            _get_routes[path] = std::move(handler);
        }

        /**
         * @brief Deregister a handler for GET path
         *
         * @param path - path
         */
        void
        deregister_get_route(const std::string& path)
        {
            std::unique_lock lock(_mutex);
            _get_routes.erase(path);
        }

        /**
         * @brief Redigster a handler for POST path
         *
         * @param path - path
         * @param handler - handler function
         */
        void
        register_post_route(const std::string& path, PostHandler &&handler)
        {
            std::unique_lock lock(_mutex);
            _post_routes[path] = std::move(handler);
        }

        /**
         * @brief Deregister a handler for POST path
         *
         * @param path - path
         */
        void
        deregister_post_route(const std::string& path)
        {
            std::unique_lock lock(_mutex);
            _post_routes.erase(path);
        }

        static bool
        exists() { return _has_instance(); }

    private:

        /**
         * @brief This class inherits from httplib::Server in order to expose an IP and port
         *  that the server binds to.
         *
         */
        class InternalHTTPServer : public httplib::Server {
        public:
            /**
             * @brief Get a string representing an ip and port that server listens on
             *
             * @return std::string
             */
            std::string get_bind_ip_port();

            /**
             * @brief Help function for dumping the content of an HTTP request
             *
             * @param request - request
             * @return std::string - output string
             */
            static std::string request_to_string(const httplib::Request& request);
        };

        InternalHTTPServer _svr;               ///< HTTP server object
        std::string _ip{"0.0.0.0"};         ///< initial IP
        uint16_t _port{0};                     ///< initial port
        std::unordered_map<std::string, GetHandler> _get_routes;        ///< map of GET routes
        std::unordered_map<std::string, PostHandler> _post_routes;      ///< map of POST routes
        std::shared_mutex _mutex;              ///< mutex for multithreaded access control to routing maps

        /**
         * @brief Construct a new Admin Server object
         *
         */
        AdminServer();

        /**
         * @brief Destroy the Admin Server object
         *
         */
        ~AdminServer() override = default;

        /**
         * @brief Signal server thread to stop serving requests and shutdown
         *
         */
        void _internal_thread_shutdown()  override { _svr.stop(); }

        /**
         * @brief Runs HTTP server in a separate thread
         *
         */
        void _internal_run() override;

        /**
         * @brief Dispatch GET requests
         *
         * @param req - request
         * @param res - response
         */
        void _dispatch_get(const httplib::Request& req, httplib::Response& res);

        /**
         * @brief Dispatch POST requests
         *
         * @param req - request
         * @param res - response
         */
        void _dispatch_post(const httplib::Request& req, httplib::Response& res);

        /**
         * @brief Generic function wrapper for handling possible errors from calling the
         *      the function passed as an argument.
         *
         * @tparam Func - template parameter of the function to be called
         * @tparam Args - template parameter for the function argument list
         * @param res - reference to HTTP response object
         * @param func - function to call
         * @param args - argument list for the function
         * @return requires - the function is required to return a JSON object
         */
        template<typename Func, typename... Args>
        requires std::same_as<std::invoke_result_t<Func, Args...>, nlohmann::json>
        void _wrap_error_handler(httplib::Response& res, Func func, Args && ...args) {
            nlohmann::json result = {};
            try {
                result = func(std::forward<Args>(args)...);
            } catch (HttpError &e) {
                res.status = e.get_error_code();
                result = {
                    {"status", "internal error"},
                    {"error_message", std::string(e.what()) }
                };
                LOG_ERROR("Internal error while processing HTTP request: {}", e.what());
            } catch (nlohmann::detail::exception &e) {
                res.status = 500;
                result = {
                    {"status", "JSON error"},
                    {"error_message", std::string(e.what()) }
                };
                LOG_ERROR("JSON error while processing HTTP request: {}", e.what());
            } catch (std::exception &e) {
                res.status = 500;
                result = {
                    {"status", "STL error"},
                    {"error_message", std::string(e.what()) }
                };
                LOG_ERROR("STL error while processing HTTP request: {}", e.what());
            } catch (...) {
                res.status = 500;
                result = {
                    {"status", "unknown error"}
                };
                LOG_ERROR("Unknown exception while processing HTTP request");
            }
            res.set_content(result.dump(), "application/json");
        }

    };
} // springtail