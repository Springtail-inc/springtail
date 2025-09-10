#include <common/json.hh>
#include <common/properties.hh>
#include <common/redis_types.hh>

#include <admin_http/admin_server.hh>

namespace springtail {

    AdminServer::AdminServer() : Singleton<AdminServer>()
    {
        // Default dispatch routes
        _svr.Get("/health",
                 []([[maybe_unused]] const httplib::Request& req, httplib::Response& res) {
                    res.set_content(R"({"status":"up"})", "application/json");
                });

        _svr.Get("/config",
                 []([[maybe_unused]] const httplib::Request& req, httplib::Response& res) {
                    nlohmann::json settings = Properties::get_all_settings();
                    std::string json_response = std::string(R"({"status":"ok", "settings":)")
                            + settings.dump() + "}";
                    res.set_content(json_response, "application/json");
                });

        // Hook dispatcher into all GET and POST requests
        _svr.Get(".*",
                 [this](const httplib::Request& req, httplib::Response& res) {
                    _dispatch_get(req, res);
                });
        _svr.Post(".*",
                  [this](const httplib::Request& req, httplib::Response& res) {
                    _dispatch_post(req, res);
                });

        // start thread
        start_thread();
        _svr.wait_until_ready();

        // Publish port in redis

        // get ip and port
        std::string ip_port = _svr.get_bind_ip_port();

        // get process name
        std::ifstream comm("/proc/self/comm");
        std::string process_name;
        std::getline(comm, process_name);

        // get instance id and instance key
        uint64_t instance_id = Properties::get_db_instance_id();
        std::string instance_key = Properties::get_instance_key();

        // create hash name and hash key
        std::string hash_name = fmt::format(redis::ADMIN_CONSOLE, instance_id);
        std::string hash_key = fmt::format("{}:{}", instance_key, process_name);

        // Update redis info
        auto [db_id, redis_client] = RedisMgr::get_instance()->create_client(true);
        redis_client->hset(hash_name, hash_key, ip_port);
    }

    void
    AdminServer::_internal_run()
    {
        _svr.listen(_ip, _port);
    }

    void
    AdminServer::_dispatch_get(const httplib::Request& req, httplib::Response& res)
    {
        GetHandler handler;
        {
            std::shared_lock lock(_mutex);
            auto it = _get_routes.find(req.path);
            if (it != _get_routes.end()) {
                handler = it->second;
            }
        }
        if (handler) {
            nlohmann::json json_res;
            handler(req.path, req.params, json_res);
            res.set_content(json_res.dump(), "application/json");
        } else {
            res.status = 404;
            res.set_content("{\"error\":\"not found\"}", "application/json");
        }
    }

    void
    AdminServer::_dispatch_post(const httplib::Request& req, httplib::Response& res)
    {
        PostHandler handler;
        {
            std::shared_lock lock(_mutex);
            auto it = _post_routes.find(req.path);
            if (it != _post_routes.end()) {
                handler = it->second;
            }
        }
        if (handler) {
            nlohmann::json json_res;
            handler(req.path, req.params, req.body, json_res);
            res.set_content(json_res.dump(), "application/json");
        } else {
            res.status = 404;
            res.set_content("{\"error\":\"not found\"}", "application/json");
        }
    }

    std::string
    AdminServer::InternalHTTPServer::get_bind_ip_port() {
        socket_t socket_fd = svr_sock_.load();
        if (socket_fd == INVALID_SOCKET) {
            return "";
        }
        sockaddr_storage addr;
        socklen_t len = sizeof(addr);

        std::string ip_port;
        // Get local (server) info
        if (getsockname(socket_fd, (sockaddr*)&addr, &len) == 0) {
            if (addr.ss_family == AF_INET) {
                sockaddr_in* s = (sockaddr_in*)&addr;
                char ip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &s->sin_addr, ip, sizeof(ip));
                ip_port = fmt::format("{}:{}", ip, s->sin_port);
            } else if (addr.ss_family == AF_INET6) {
                sockaddr_in6* s = (sockaddr_in6*)&addr;
                char ip[INET6_ADDRSTRLEN];
                inet_ntop(AF_INET6, &s->sin6_addr, ip, sizeof(ip));
                ip_port = fmt::format("{}:{}", ip, s->sin6_port);
            }
        }
        return ip_port;
    }

    std::string
    AdminServer::InternalHTTPServer::request_to_string(const httplib::Request& request)
    {
        std::ostringstream ss;
        ss << "Request:\n";
        ss << "\tmethod: " << request.method << "\n";
        ss << "\tpath: " << request.path << "\n";
        ss << "\tmatched_route: " << request.matched_route << "\n";
        ss << "\tparams:\n";
        for (auto &[k, v]: request.params) {
            ss << "\t\t" << k << " : " << v << "\n";
        }
        ss << "\theaders:\n";
        for (auto &[k, v]: request.headers) {
            ss << "\t\t" << k << " : " << v << "\n";
        }
        ss << "\ttrailers:\n";
        for (auto &[k, v]: request.trailers) {
            ss << "\t\t" << k << " : " << v << "\n";
        }
        ss << "\tbody: " << request.body << "\n";
        ss << "\tversion: " << request.version << "\n";
        ss << "\ttarget: " << request.target << "\n";
        ss << "\tform:\n";
        ss << "\t\tfields:\n";
        for (auto &[k, v]: request.form.fields) {
            ss << "\t\t\t" << k << ":\n";
            ss << "\t\t\t\tname: " << v.name << "\n";
            ss << "\t\t\t\tcontent: " << v.content << "\n";
            ss << "\t\t\t\theaders:\n";
            for (auto &[vk, vv]: v.headers) {
                ss << "\t\t\t\t\t" << vk << " : " << vv << "\n";
            }
        }
        ss << "\t\tfiles:\n";
        for (auto &[k, v]: request.form.files) {
            ss << "\t\t\t" << k << ":\n";
            ss << "\t\t\t\tname: " << v.name << "\n";
            ss << "\t\t\t\tcontent: " << v.content << "\n";
            ss << "\t\t\t\tfilename: " << v.filename << "\n";
            ss << "\t\t\t\tcontent_type: " << v.content_type << "\n";
            ss << "\t\t\t\theaders:\n";
            for (auto &[vk, vv]: v.headers) {
                ss << "\t\t\t\t\t" << vk << " : " << vv << "\n";
            }
        }
        ss << "\tranges:\n";
        for (auto &[f, s]: request.ranges) {
            ss << "\t\t" << f << ", " << s << "\n";
        }

        ss << "\tpath_params:\n";
        for (auto &[k, v]: request.path_params) {
            ss << "\t\t" << k << " : " << v << "\n";
        }
        return ss.str();
    }

} // springtail