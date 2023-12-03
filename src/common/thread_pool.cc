#include <common/thread_pool.hh>

#include <iostream>

namespace springtail {

    /** This function is a static worker function, the actual worker is passed in */
    template <class T> void
    ThreadWorker<T>::worker_fn(std::shared_ptr<ThreadWorker<T>> worker,
                               ThreadQueue<T> &queue)
    {
        std::cout << "Thread starting: " << std::this_thread::get_id() << std::endl;
        while (true) {
            std::shared_ptr<T> request = queue.pop();
            // only get a nullptr if queue is empty, if so check for shutdown
            if (request == nullptr) {
                if (worker->is_shutdown()) {
                    return;
                }
                continue;
            }

            // process request
            (*request)();
        }
        std::cout << "Thread exiting: " << std::this_thread::get_id() << std::endl;        
    }

    template <class T> void
    ThreadQueue<T>::push(std::shared_ptr<T> request)
    {
        // lock queue lock
        std::scoped_lock<std::mutex> queue_lock(_mutex);

        // push request onto queue and notify a single thread
        _queue.push(request);
        _cv.notify_one();
    }

    template <class T> std::shared_ptr<T>
    ThreadQueue<T>::pop()
    {
        // lock queue lock
        std::unique_lock<std::mutex> queue_lock(_mutex);
        
        // block until queue is not empty
        // or until queue is signalled to wake all
        while (_queue.empty() && !_shutdown) {
            _cv.wait(queue_lock);
        }

        // extract first element from queue and return it
        if (_queue.empty()) {
            return nullptr;
        }

        std::shared_ptr<T> val = _queue.front();
        _queue.pop();

        return val;
    }
    
    template <class T>
    ThreadPool<T>::ThreadPool(int max_threads)
    {
        for (int i = 0; i < max_threads; i++) {
            std::shared_ptr<ThreadWorker<T>> worker = std::make_shared<ThreadWorker<T>>();
            _workers.push_back(worker);
            _threads.push_back(std::thread(worker->get_worker_fn(), worker, std::ref(_queue)));
        }
    }

    template <class T> void
    ThreadPool<T>::queue(std::shared_ptr<T> request)
    {
        if (_shutdown) {
            throw Error("Pool has shutdown\n");
        }
        _queue.push(request);
    }

    template <class T> void
    ThreadPool<T>::shutdown()
    {
        if (_shutdown) {
            return;
        }

        // set shutdown flag
        _shutdown = true;

        // issue shutdown to workers
        for (auto worker: _workers) {
            worker->shutdown();
        }

        // issue shutdown to queue (this will drain the queue)
        _queue.shutdown();

        // join the threads and remove them from the vector
        while (!_threads.empty()) {
            std::thread &t = _threads.back();
            std::cout << "Thread joining: " << t.get_id() << std::endl;
            t.join();
            _threads.pop_back();
        }
    }

} // namespace springtail

#include <iostream>
class TestRequest {
public:
    int _a;
    TestRequest(int a) : _a(a) {}
    void operator()() {
        std::cout << "Request is working..." << _a << "\n";
    }
};


int main(void)
{
    springtail::ThreadPool<TestRequest> pool(2);

    pool.queue(std::make_shared<TestRequest>(1));
    pool.queue(std::make_shared<TestRequest>(2));
    pool.queue(std::make_shared<TestRequest>(3));
    pool.queue(std::make_shared<TestRequest>(4));

    pool.shutdown();    
}