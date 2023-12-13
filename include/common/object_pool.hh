#include <memory>
#include <string>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <cassert>

namespace springtail {

    template <class T>
    class ObjectPool {
    public:
        /**
         * @brief Construct an Object Pool object and populate with start objects
         * @param creator_fn   Function for creating objects of type T
         * @param start        Starting sockets in pool
         * @param max          Max sockets in pool
         */
        ObjectPool(std::function<std::shared_ptr<T>()> creator_fn, int start, int max)
            : _max(max), _outstanding(0), _creator_fn(creator_fn)
        {
            // initialize queue with starting set of channels
            for (int i = 0; i < start; i++) {
                _queue.push(_creator_fn());
            }        
        }
       
        /**
         * @brief Get object from pool (queue); block if none available (if outstanding >= max)
         * @return std::shared_ptr<T> 
         */
        std::shared_ptr<T> get()
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            
            // if queue is empty and are above or at max limit wait
            while (_queue.empty() && (_outstanding >= _max || !_creator_fn)) {
                _cv.wait(queue_lock);
            }

            // if queue is empty and we are below max, create socket
            if (_queue.empty() && _outstanding < _max) {
                _outstanding++;
                return _creator_fn();
            }

            // otherwise get a socket from the queue
            std::shared_ptr<T> obj = _queue.front();
            _queue.pop();
            _outstanding++;
            return obj;
        }

        /**
         * @brief Release socket back to queue
         * @param socket to release
         */
        void put(std::shared_ptr<T> obj)
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            if (_outstanding + _queue.size() <= _max) {
                _queue.push(obj);
                _cv.notify_one();
            }
            _outstanding--;
            cassert(_outstanding + _queue.size() <= _max);
        }



    private:
        /** max number of sockets in pool */
        int _max;
        /** number of outstanding sockets */
        int _outstanding;

        /** function for creating objects */
        std::function<std::shared_ptr<T>()> _creator_fn;
      
        /** mutex protecting queue cv */
        std::mutex _mutex;
        /** condition variable for blocking on queue */
        std::condition_variable _cv;

        /** queue of sockets (socket pool)*/
        std::queue<std::shared_ptr<T>> _queue;
    };
}