#include <nlohmann/json.hpp>
#include <zmq.hpp>

#include <thread>

#include <common/properties.hh>
#include <common/json.hh>

#include <write_cache/write_cache_server.hh>

namespace springtail {
    

    /* static initialization must happen outside of class */
    WriteCacheServer* WriteCacheServer::_instance {nullptr};
    std::mutex WriteCacheServer::_instance_mutex;

    WriteCacheServer *
    WriteCacheServer::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new WriteCacheServer();
        }

        return _instance;
    }

    WriteCacheServer::WriteCacheServer() : _service(std::make_shared<WriteCacheService>())
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;        
        
        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        if (!Json::get_to<std::string>(server_json, "host", _server_host)) {
            throw Error("Write cache 'server.host' setting not found");
        }

        Json::get_to<int>(server_json, "worker_threads", _worker_thread_count, 8);
        Json::get_to<int>(server_json, "io_threads", _io_thread_count, 2);
    }

    static void
    worker_fn(std::shared_ptr<zmq::context_t> context,
              std::shared_ptr<WriteCacheService> service)
    {
        // create socket and connect to in process workers socket
        zmq::socket_t socket(*context, ZMQ_REP);
        socket.connect("inproc://workers");

        while (true) {
            zmq::message_t request;
            zmq::recv_result_t res = socket.recv(request, zmq::recv_flags::none);   // data in request.data
            if (res == -1) {
                if (errno == ETERM) {
                    // shutdown
                    return;
                }
            }

            // do work

            zmq::message_t reply(6); // set reply.data()
            socket.send(reply, zmq::send_flags::dontwait);
        }
    }

    /** 
     * Startup the zeromq server, for an explanation see: 
     * https://thisthread.blogspot.com/2011/08/multithreading-with-zeromq.html
     */
    void
    WriteCacheServer::startup()
    {
        // setup zmq context
        _context = std::make_shared<zmq::context_t>(_io_thread_count);
        
        // setup a connection between client socket and workers socket
        zmq::socket_t clients(*_context, ZMQ_ROUTER);
        clients.bind("tcp://" + _server_host);
        zmq::socket_t workers(*_context, ZMQ_DEALER);
        workers.bind("inproc://workers");

        // docs not clear on this, not sure if we should use this
        // zmq_device(ZMQ_QUEUE, clients, workers); // this looks deprecated
        // or this
        zmq::proxy(clients, workers);

        // create worker threads
        for (int i = 0; i < _worker_thread_count; i++) {
            _workers.push_back(std::thread(worker_fn, _context, _service));
        }

        // block waiting for threads to complete
        for (int i = 0; i < _worker_thread_count; i++) {
            _workers[i].join();
        }
    }

    void
    WriteCacheServer::shutdown()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            _instance->_context->shutdown();
            delete _instance;
            _instance = nullptr;
        }
    }

    
}

int main (void)
{

}