#include <memory>
#include <string>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <cassert>

namespace springtail {

    /**
     * @brief Factory class -- provides allocator, deallocator and get callback for ObjectPool
     * @tparam T type of object being returned by the object pool
     */
    template <class T>
    class ObjectPoolFactory
    {
    public:
        virtual ~ObjectPoolFactory() {}

        virtual std::shared_ptr<T> allocate()=0;
        virtual void deallocate(std::shared_ptr<T> obj) {};
        virtual void get_cb(std::shared_ptr<T> obj) {};
    };

    /**
     * @brief Object pool; caches a set of objects that can be fetched and returned to the pool
     * @tparam T type of object being returned by the pool
     */
    template <class T>
    class ObjectPool {
    public:
        /**
         * @brief Construct an Object Pool object and populate with start objects
         * @param creator_fn   Function for creating objects of type T
         * @param start        Starting sockets in pool
         * @param max          Max sockets in pool
         */
        ObjectPool(std::shared_ptr<ObjectPoolFactory<T>> factory, int start, int max)
            : _max(max), _outstanding(0), _factory(factory)
        {
            // initialize queue with starting set of channels
            for (int i = 0; i < start; i++) {
                _queue.push(_factory->allocate());
            }        
        }
        /**
         * @brief Get object from pool (queue); block if none available (if outstanding >= max)
         * @return std::shared_ptr<T> 
         */    
        std::shared_ptr<T> get()
        {
            std::shared_ptr<T> obj = _get(); // get obj
            _factory->get_cb(obj); // do get callback with factory, don't want to be holding lock
            return obj;
        }

        /**
         * @brief Release socket back to queue
         * @param obj to release
         */
        void put(std::shared_ptr<T> obj)
        {
            if (!_put(obj)) {
                _factory->deallocate(obj);
            }
        }

    private:
        /**
         * @brief Get object from pool (queue); block if none available (if outstanding >= max)
         * @return std::shared_ptr<T> 
         */
        std::shared_ptr<T> _get()
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            
            // if queue is empty and are above or at max limit wait
            while (_queue.empty() && (_outstanding >= _max)) {
                _cv.wait(queue_lock);
            }

            // if queue is empty and we are below max, create socket
            if (_queue.empty() && _outstanding < _max) {
                _outstanding++;
                return _factory->allocate();
            }

            // otherwise get a socket from the queue
            std::shared_ptr<T> obj = _queue.front();
            _queue.pop();
            _outstanding++;
            return obj;
        }

        /**
         * @brief Release socket back to queue
         * @param obj to release
         * @returns true if object is requeued, false otherwise
         */
        bool _put(std::shared_ptr<T> obj)
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            if (_outstanding + _queue.size() <= _max) {
                _queue.push(obj);
                _cv.notify_one();
                _outstanding--;
                return true;
            } else {
                _outstanding--;                
                return false;
            }
        }

        /** max number of sockets in pool */
        int _max;
        /** number of outstanding sockets */
        int _outstanding;

        /** factory for creating and destroying objects in the pool */
        std::shared_ptr<ObjectPoolFactory<T>> _factory;
      
        /** mutex protecting queue cv */
        std::mutex _mutex;
        /** condition variable for blocking on queue */
        std::condition_variable _cv;

        /** queue of sockets (socket pool)*/
        std::queue<std::shared_ptr<T>> _queue;
    };
}