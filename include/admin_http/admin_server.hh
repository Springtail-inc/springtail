#pragma once

#include <shared_mutex>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <common/singleton.hh>

namespace springtail {

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
    };
} // springtail