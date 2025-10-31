#include <proxy/buffer_pool.hh>

namespace springtail::pg_proxy {

    /* static initialization must happen outside of class */
    BufferPool* BufferPool::_instance{nullptr};

    std::once_flag BufferPool::_init_flag;

    BufferPool *
    BufferPool::_init()
    {
        _instance = new BufferPool();
        return _instance;
    }

    BufferPool *
    BufferPool::get_instance() {
        std::call_once(_init_flag, _init);
        return _instance;
    }

    BufferPtr
    BufferPool::get(int size)
    {
        // if size is > BIG_ALLOC_QUEUE_LIMIT, block the thread on the alloc_queue until cv is notified
        if (size >= BIG_ALLOC_QUEUE_LIMIT) {
            std::unique_lock<std::mutex> lock(_alloc_mutex);

            // if there is already an allocation outstanding, wait for it to complete
            if (_big_alloc_count >= BIG_ALLOCS_ALLOWED) {
                WaiterPtr waiter = std::make_shared<Waiter>();
                _big_alloc_queue.push(waiter);
                waiter->cv.wait(lock, [waiter] { return waiter->done; });
            }
            _big_alloc_count++;
        }

        // can't use make_shared because constructor is private
        return Buffer::create(size);
    }

    void
    BufferPool::release(int size)
    {
        if (size < BIG_ALLOC_QUEUE_LIMIT) {
            return;
        }

        // if size is > BIG_ALLOC_QUEUE_LIMIT, notify a waiting thread, if there is one
        std::unique_lock<std::mutex> lock(_alloc_mutex);
        _big_alloc_count--;
        if (_big_alloc_queue.size() == 0) {
            assert (_big_alloc_count < BIG_ALLOCS_ALLOWED); // should be 0 if no one is waiting
            return;
        }

        // notify a waiting thread
        WaiterPtr waiter = _big_alloc_queue.front();
        _big_alloc_queue.pop();
        lock.unlock();
        waiter->done = true;
        waiter->cv.notify_one();
    }
} // namespace springtail::pg_proxy
