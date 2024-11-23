#pragma once

#include <atomic>
#include <mutex>
#include <thread>

namespace springtail {
    // TODO: add documentation
    template <typename T> class Singleton {
    public:
        static T *get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        // delete copy
        Singleton(const Singleton&) = delete;
        Singleton& operator=(const Singleton&) = delete;

        // delete move
        Singleton(Singleton&&) = delete;
        Singleton& operator=(Singleton&&) = delete;

        static void shutdown() {
            std::call_once(_shutdown_flag, _shutdown);
        }

    protected:
        // to be overridden by the derived class
        virtual void _internal_shutdown() {}
        virtual ~Singleton() {}
        Singleton() {}

    private:
        static inline T* _instance = nullptr;
        static inline std::once_flag _init_flag;          ///< initialization flag
        static inline std::once_flag _shutdown_flag;      ///< shutdown flag

        static void _init() {
            if (_instance == nullptr) {
                _instance = new T();
            }
        }

        static void _shutdown() {
            if (_instance != nullptr) {
                _instance->_internal_shutdown();
                delete _instance;
                _instance = nullptr;
            }
        }
    };

    template <typename T> class SingletonWithThread {
    public:
        static T *get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        // delete copy
        SingletonWithThread(const SingletonWithThread&) = delete;
        SingletonWithThread& operator=(const SingletonWithThread&) = delete;

        // delete move
        SingletonWithThread(SingletonWithThread&&) = delete;
        SingletonWithThread& operator=(SingletonWithThread&&) = delete;

        static void shutdown() {
            std::call_once(_shutdown_flag, _shutdown);
        }

        void start_thread() {
            _thread = std::thread(&T::_internal_run, (T *)this);
        }

        void stop_thread() {
            _shutting_down = true;
        }

    protected:
        // to be overridden by the derived class
        virtual void _internal_shutdown() {}
        virtual void _internal_run() {}
        bool _is_shutting_down() { return _shutting_down; }
        virtual ~SingletonWithThread() {}
        SingletonWithThread() {}

    private:
        static inline T* _instance = nullptr;
        static inline std::once_flag _init_flag;          ///< initialization flag
        static inline std::once_flag _shutdown_flag;      ///< shutdown flag
        std::thread _thread;
        std::atomic<bool> _shutting_down = false;

        static void _init() {
            if (_instance == nullptr) {
                _instance = new T();
            }
        }

        static void _shutdown() {
            if (_instance != nullptr) {
                _instance->_thread.join();
                _instance->_internal_shutdown();
                delete _instance;
                _instance = nullptr;
            }
        }
    };

};