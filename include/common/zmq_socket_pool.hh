#include <memory>
#include <string>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <cassert>

#include <zmq.hpp>

namespace springtail {

    class ZmqSocketPool {
    public:
        /**
         * @brief Construct a new Zmq Socket Pool object
         * @param context      Zero mq context
         * @param server_addr  Remote server address "host:port"; type tcp assumed
         * @param start        Starting sockets in pool
         * @param max          Max sockets in pool
         */
        ZmqSocketPool(std::shared_ptr<zmq::context_t> context, const std::string &server_addr, int start, int max)
            : _max(max), _outstanding(0), _server_addr(server_addr), _context(context)
        {
            // initialize queue with starting set of channels
            for (int i = 0; i < start; i++) {
                _queue.push(_create());
            }
        }

        /**
         * @brief Get socket from pool (queue); block if none available (if outstanding >= max)
         * @return std::shared_ptr<zmq::socket_t> 
         */
        std::shared_ptr<zmq::socket_t> get()
        {
            std::shared_ptr<zmq::socket_t> socket = _get();
            if (!*socket) { // check if connected
                socket->connect(_server_addr);
            }
            return socket;
        } 

        /**
         * @brief Release socket to queue
         * @param socket to release
         */
        void put(std::shared_ptr<zmq::socket_t> socket)
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            if (_outstanding + _queue.size() > _max) {
                // close socket
                socket->close();
            } else {
                _queue.push(socket);
                _cv.notify_one();
            }
            _outstanding--;
        }
    
    private:
        /** max number of sockets in pool */
        int _max;
        /** number of outstanding sockets */
        int _outstanding;
        /** remote server address */
        std::string _server_addr;
      
        /** zeromq context */
        std::shared_ptr<zmq::context_t> _context;
        /** mutex protecting queue cv */
        std::mutex _mutex;
        /** condition variable for blocking on queue */
        std::condition_variable _cv;
        /** queue of sockets (socket pool)*/
        std::queue<std::shared_ptr<zmq::socket_t>> _queue;

        /**
         * @brief Get socket from pool (queue); block if none available (if outstanding >= max)
         * @return std::shared_ptr<zmq::socket_t>; may not be connected yet
         */
        std::shared_ptr<zmq::socket_t> _get()
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            
            // if queue is empty and are above or at max limit wait
            while (_queue.empty() && _outstanding >= _max) {
                _cv.wait(queue_lock);
            }

            // if queue is empty and we are below max, create socket
            if (_queue.empty() && _outstanding < _max) {
                _outstanding++;
                // even though zmq may do lazy connect, we
                // don't do the connect here since we are holding
                // a lock and don't want to block further
                return _create(false);
            }

            // otherwise get a socket from the queue
            std::shared_ptr<zmq::socket_t> socket = _queue.front();
            _queue.pop();
            _outstanding++;
            return socket;
        }

        /**
         * @brief Create a zmq::socket_t
         * @param connect default true; do the socket connect; this may be lazy by zmq
         * @return std::shared_ptr<zmq::socket_t> 
         */
        inline std::shared_ptr<zmq::socket_t> _create(bool connect=true)
        {
            std::shared_ptr<zmq::socket_t> socket = std::make_shared<zmq::socket_t>(*_context, ZMQ_REQ);
            socket->set(zmq::sockopt::immediate, true);
            socket->set(zmq::sockopt::tcp_keepalive, 1);
            socket->connect("tcp://" + _server_addr);
            return socket;
        }
    };
}