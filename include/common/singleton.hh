#pragma once

#include <atomic>
#include <mutex>

namespace springtail {
    // TODO: add documentation
    template <typename T, typename ... Args> class Singleton {
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

        // to be overridden by the derived class
        virtual void startup(Args ... args) {};

        void wait_shutdown() {
            _stop();
            if (_instance != nullptr) {
                delete _instance;
                _instance = nullptr;
            }
        }

    protected:
        // to be overridden by the derived class
        virtual void _stop() {};
        bool _is_shutting_down() { return _shutting_down; }
        virtual ~Singleton() {}
        Singleton() {}

    private:
        static inline T* _instance = nullptr;
        static inline std::once_flag _init_flag;          ///< initialization flag
        static inline std::once_flag _shutdown_flag;      ///< shutdown flag
        std::atomic<bool> _shutting_down {false};  ///< shutdown flag, set in shutdown()

        static void _init() {
            _instance = new T();
        }

        static void _shutdown() {
            if (_instance == nullptr) {
                return;
            }
            _instance->_internal_shutdown();
        }

        void _internal_shutdown() {
            _shutting_down = true;
        }
    };
};