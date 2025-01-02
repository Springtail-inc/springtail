#include <thrift_override/concurrency/ThreadManager.h>
#include <thrift/thrift-config.h>
#include <thrift/concurrency/Exception.h>

#include <memory>
#include <stdexcept>
#include <deque>
#include <set>

#include <iostream>

#include <common/concurrent_queue.hh>

namespace apache {
namespace thrift {
namespace concurrency {

using std::shared_ptr;
using std::unique_ptr;

class ThreadManager::Impl : public ThreadManager {
public:
    Impl() :
        state_(UNINITIALIZED),
        pendingTaskCountMax_(0),
        workerCount_(0),
        expiredCount_(0),
        expireCallback_(nullptr),
        tasks_(-1) {}
    ~Impl() override {
        if (state_ != STOPPED) {
            stop();
        }
    }

    ThreadManager::STATE state() const override { return state_; }
    size_t pendingTaskCountMax() override { return pendingTaskCountMax_; }
    void pendingTaskCountMax(const size_t value) {
        pendingTaskCountMax_ = value;
    }

    shared_ptr<ThreadFactory> threadFactory() override {
        std::unique_lock lock(mutex_);
        return threadFactory_;
    }

    void threadFactory(shared_ptr<ThreadFactory> value) override {
        std::unique_lock lock(mutex_);
        if (threadFactory_ && threadFactory_->isDetached() != value->isDetached()) {
            throw InvalidArgumentException();
        }
        threadFactory_ = value;
    }

    size_t idleWorkerCount() override {
        std::unique_lock lock(mutex_);
        return countIdleWorkers();
    }

    size_t workerCount() override {
        std::unique_lock lock(mutex_);
        return workerCount_;
    }

    size_t pendingTaskCount() override {
        std::unique_lock lock(mutex_);
        return tasks_.size();
    }

    size_t totalTaskCount() override {
        std::unique_lock lock(mutex_);
        return tasks_.size() + workerCount_ - countIdleWorkers();
    }

    // unsupported
    size_t expiredTaskCount() override {
        return 0;
    }

    void setExpireCallback(ExpireCallback expireCallback) {
        std::unique_lock lock(mutex_);
        expireCallback_ = expireCallback;
    }

    // Worker functions
    void addWorker(size_t value) override;

    // this function will block if the tasks are long running
    void removeWorker(size_t value) override;

    // Task functions
    // timeout and expiration are unsupported
    void add(shared_ptr<Runnable> value, int64_t timeout, int64_t expiration) override;
    // unsupported
    void remove(shared_ptr<Runnable> task) override {}

    std::shared_ptr<Runnable> removeNextPending() override;

    // unsupported
    void removeExpiredTasks() override {
    }

    // thread functions
    void start() override;
    void stop() override;

private:
    std::atomic<ThreadManager::STATE> state_;
    std::atomic<size_t> pendingTaskCountMax_;
    size_t workerCount_;
    size_t expiredCount_;
    ExpireCallback expireCallback_;

    shared_ptr<ThreadFactory> threadFactory_;
    springtail::ConcurrentQueue<Task> tasks_;
    std::mutex mutex_;

    std::set<shared_ptr<Thread>> pendingWorkers_;     // workers that were added before the mananger started
    std::map<const Thread::id_t, shared_ptr<Thread> > idMap_;

    std::map<const Thread::id_t, shared_ptr<Thread>>::iterator stopAndRemoveThread(std::map<const Thread::id_t, shared_ptr<Thread>>::iterator &it);
    size_t countIdleWorkers();
};

class ThreadManager::Task {
public:
    ~Task() = default;

    // not supporting expiration time
    Task(shared_ptr<Runnable> runnable) :
        runnable_(runnable) {}

    shared_ptr<Runnable> getRunnable() { return runnable_; }

private:
    shared_ptr<Runnable> runnable_;
};

class ThreadManager::Worker : public Runnable {
public:
    enum STATE { UNINITIALIZED, STARTING, STARTED, STOPPING, STOPPED };
    Worker(ThreadManager::Impl* manager)
        : manager_(manager), state_(UNINITIALIZED), threadId_((std::thread::id)(0)) {
            idle_ = true;
        }
    ~Worker() override = default;
    void run() override {
        state_ = STARTING;
        while (threadId_ == (std::thread::id)(0)) {
            threadId_.wait((std::thread::id)(0));
        }
        state_ = STARTED;
        while (state_ != STOPPING) {
            std::unique_lock<std::mutex> lock(mutex_);
            shared_ptr<Runnable> task = manager_->removeNextPending();
            while (task == nullptr && state_ != STOPPING) {
                condVar_.wait(lock);
                task = manager_->removeNextPending();
            }
            if (task != nullptr) {
                idle_ = false;
                lock.unlock();
                task->run();
                lock.lock();
                idle_ = true;
            }
        }
        state_ = STOPPED;
    }
    void stop() {
        if (state_ == STARTED) {
            state_ = STOPPING;
            condVar_.notify_one();
        }
    }
    void notify() {
        condVar_.notify_one();
        GlobalOutput.printf("%s: notified idle worker %u that task is ready", __FUNCTION__, threadId_.load());
    }

    void setThreadId(const Thread::id_t threadId) {
        threadId_ = threadId;
        threadId_.notify_one();
    }

private:
    friend class ThreadManager::Impl;
    ThreadManager::Impl* manager_;
    std::atomic<STATE> state_;
    std::mutex mutex_;
    std::condition_variable condVar_;
    std::atomic<Thread::id_t> threadId_;
    std::atomic<bool> idle_;
};

void ThreadManager::Impl::addWorker(size_t value) {
    // do not add any more threads if we are already stopping
    if (state_ > STARTED) {
        return;
    }

    std::set<shared_ptr<Thread> > newThreads;
    for (size_t ix = 0; ix < value; ix++) {
        shared_ptr<ThreadManager::Worker> worker = std::make_shared<ThreadManager::Worker>(this);
        GlobalOutput.printf("worker address = %x", worker.get());
        shared_ptr<Thread> thread = threadFactory_->newThread(dynamic_pointer_cast<Runnable, ThreadManager::Worker>(worker));
        newThreads.insert(thread);
    }

    std::unique_lock<std::mutex> lock(mutex_);
    // create threads, but do not start them
    workerCount_ += value;

    // if thread manager is already running, start the threads
    if (state_ == STARTED) {
        for (const auto & newThread : newThreads) {
            shared_ptr<ThreadManager::Worker> worker
                = dynamic_pointer_cast<ThreadManager::Worker, Runnable>(newThread->runnable());
            newThread->start();
            Thread::id_t newThreadId = newThread->getId();
            worker->setThreadId(newThreadId);
            idMap_.insert(std::pair<const Thread::id_t, shared_ptr<Thread> >(newThreadId, newThread));
        }
    } else {
        pendingWorkers_.insert(newThreads.begin(), newThreads.end());
    }

}

void ThreadManager::Impl::removeWorker(size_t value) {
    // do not anything if we are already stopping
    // all the threads will be removed at the end
    if (state_ > STARTED) {
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (value > workerCount_) {
        throw InvalidArgumentException();
    }

    // stop threads and remove them
    size_t workersRemoved = 0;
    // remove idle workers first
    for (auto it = idMap_.begin(); it != idMap_.end() && workersRemoved < value;) {
        shared_ptr<ThreadManager::Worker> worker
            = dynamic_pointer_cast<ThreadManager::Worker, Runnable>(it->second->runnable());
        if (worker->idle_) {
            it = stopAndRemoveThread(it);
            workersRemoved++;
        } else {
            it++;
        }
    }
    if (workersRemoved < value) {
        for (auto it = idMap_.begin(); it != idMap_.end() && workersRemoved < value;) {
            it = stopAndRemoveThread(it);
            workersRemoved++;
        }
    }
    workerCount_ -= value;
}

void ThreadManager::Impl::add(shared_ptr<Runnable> value, int64_t timeout, int64_t expiration) {
    if (state_ > STARTED) {
        return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    if (state_ != STARTED) {
        throw IllegalStateException(
            "ThreadManager::Impl::add ThreadManager not started");
    }

    if (pendingTaskCountMax_ > 0 && (tasks_.size() >= pendingTaskCountMax_)) {
        throw TooManyPendingTasksException();
    }
    shared_ptr<ThreadManager::Task> task = std::make_shared<ThreadManager::Task>(value);
    tasks_.push(task);
}

std::shared_ptr<Runnable> ThreadManager::Impl::removeNextPending() {
    shared_ptr<ThreadManager::Task> task = tasks_.pop();
    if (task == nullptr) {
        return std::shared_ptr<Runnable>();
    }
    return task->getRunnable();
}

void ThreadManager::Impl::start() {
    if (state_ != UNINITIALIZED) {
        throw IllegalStateException(
            "ThreadManager::Impl::start ThreadManager can only be started from UNINITIALIZED state");
    }
    std::unique_lock<std::mutex> lock(mutex_);
    state_ = STARTING;

    // start all pending threads
    for (const auto & thread : pendingWorkers_) {
        shared_ptr<ThreadManager::Worker> worker
            = dynamic_pointer_cast<ThreadManager::Worker, Runnable>(thread->runnable());
        thread->start();
        Thread::id_t threadId = thread->getId();
        worker->setThreadId(threadId);
        idMap_.insert(std::pair<const Thread::id_t, shared_ptr<Thread> >(threadId, thread));
    }
    pendingWorkers_.clear();
    state_ = STARTED;
    lock.unlock();
}

void ThreadManager::Impl::stop() {
    GlobalOutput.printf("%s: called", __FUNCTION__);
    if (state_ < STARTED) {
        throw IllegalStateException(
            "ThreadManager::Impl::stop ThreadManager not started");
    }
    if (state_ > STARTED) {
        throw IllegalStateException(
            "ThreadManager::Impl::stop has been already called");
    }
    state_ = JOINING;

    std::unique_lock<std::mutex> lock(mutex_);
    tasks_.clear();
    tasks_.shutdown();

    // join all the rest of the threads and remove them
    GlobalOutput.printf("%s: idMap count = %d", __FUNCTION__, idMap_.size());
    for (auto it = idMap_.begin(); it != idMap_.end();) {
        it = stopAndRemoveThread(it);
    }
    state_ = STOPPED;
}

std::map<Thread::id_t, shared_ptr<Thread>>::iterator ThreadManager::Impl::stopAndRemoveThread(std::map<Thread::id_t, shared_ptr<Thread>>::iterator &it) {
    shared_ptr<Thread> thread = it->second;
    shared_ptr<ThreadManager::Worker> worker
        = dynamic_pointer_cast<ThreadManager::Worker, Runnable>(thread->runnable());
    worker->stop();
    if (!threadFactory_->isDetached()) {
        GlobalOutput.printf("%s: joining thread with id = %u", __FUNCTION__, it->first);
        thread->join();
    }
    return idMap_.erase(it);
}

size_t ThreadManager::Impl::countIdleWorkers() {
    size_t idleCount = 0;
    for(auto const &pair: idMap_) {
        shared_ptr<ThreadManager::Worker> worker
            = dynamic_pointer_cast<ThreadManager::Worker, Runnable>(pair.second->runnable());
        idleCount += (worker->idle_)? 1 : 0;
    }
    return idleCount;
}

class SimpleThreadManager : public ThreadManager::Impl {
public:
    SimpleThreadManager(size_t workerCountValue = 4, size_t pendingTaskCountMaxValue = 0)
            : workerCount_(workerCountValue), pendingTaskCountMax_(pendingTaskCountMaxValue) {
        GlobalOutput.printf("%s: worker_count_ = %d, pendingTaskCountMax_ = %d",
            __FUNCTION__, workerCountValue, pendingTaskCountMaxValue);
    }
  void start() override {
    ThreadManager::Impl::pendingTaskCountMax(pendingTaskCountMax_);
    ThreadManager::Impl::start();
    addWorker(workerCount_);
  }

private:
  const size_t workerCount_;
  const size_t pendingTaskCountMax_;
};

shared_ptr<ThreadManager> ThreadManager::newThreadManager() {
    return shared_ptr<ThreadManager>(new ThreadManager::Impl());
}

shared_ptr<ThreadManager> ThreadManager::newSimpleThreadManager(size_t count,
                                                                size_t pendingTaskCountMax) {
    GlobalOutput.printf("this is my ThreadManager: %s", __FUNCTION__);
    return shared_ptr<ThreadManager>(new SimpleThreadManager(count, pendingTaskCountMax));
}

}
}
} // apache::thrift::concurrency
