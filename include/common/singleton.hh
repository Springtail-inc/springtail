#pragma once

#include <atomic>
#include <cassert>
#include <mutex>
#include <thread>

namespace springtail {
    template <typename T> class Singleton {
    public:
        /**
         * @brief Get the instance template object. Calls _init() that creates the object only once
         *
         * @return the pointer to the derived class T
         */
        static T *get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        // Copy constructor and assignment operator are deleted
        Singleton(const Singleton&) = delete;
        Singleton& operator=(const Singleton&) = delete;

        // Move constructor and move operator are deleted
        Singleton(Singleton&&) = delete;
        Singleton& operator=(Singleton&&) = delete;

        /**
         * @brief Shutdown function will only perform shutdown once
         *
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, _shutdown);
        }

    protected:
        /**
         * @brief This function is intended to be provided by the derived class to perform
         *          its own cleanup.
         *
         */
        virtual void _internal_shutdown() {}

        /**
         * @brief Constructor of a new Singleton object can only be accessed by the derived class
         *
         */
        Singleton() = default;

        /**
         * @brief Destructor of the Singleton object can only be accessed by the derived class
         *
         */
        virtual ~Singleton() = default;

        /**
         * @brief Assert if the singleton object has not been created yet
         *
         */
        static void _assert_instance() {
            assert(_instance != nullptr);
        }

    private:
        static inline T* _instance = nullptr;             ///< derived class instance
        static inline std::once_flag _init_flag;          ///< initialization flag
        static inline std::once_flag _shutdown_flag;      ///< shutdown flag

        /**
         * @brief Object creation function
         *
         */
        static void _init() {
            if (_instance == nullptr) {
                _instance = new T();
            }
        }

        /**
         * @brief Object cleanup function
         *
         */
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
        /**
         * @brief Get the instance template object. Calls _init() that creates the object only once
         *
         * @return the pointer to the derived class T
         */
        static T *get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        // Copy constructor and assignment operator are deleted
        SingletonWithThread(const SingletonWithThread&) = delete;
        SingletonWithThread& operator=(const SingletonWithThread&) = delete;

        // Move constructor and move operator are deleted
        SingletonWithThread(SingletonWithThread&&) = delete;
        SingletonWithThread& operator=(SingletonWithThread&&) = delete;

        /**
         * @brief Shutdown function will only perform shutdown once
         *
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, _shutdown);
        }

        /**
         * @brief Start the thread
         *
         */
        void start_thread() {
            _thread = std::thread(&T::_internal_run, (T *)this);
        }

        /**
         * @brief Stop the thread
         *
         */
        virtual void stop_thread() {
            _shutting_down = true;
        }

    protected:
        /**
         * @brief This function is intended to be provided by the derived class to perform
         *          its own cleanup.
         *
         */
        virtual void _internal_shutdown() {}

        /**
         * @brief This function is intended to be provided by the derived class to be run
         *          in the thread.
         *
         */
        virtual void _internal_run() {}

        /**
         * @brief This function is to be called by the derived class to check if the thread
         *          needs to stop.
         *
         * @return true
         * @return false
         */
        bool _is_shutting_down() const { return _shutting_down; }

        /**
         * @brief Constructor of a new SingletonWithThread object can only be accessed by the derived class
         *
         */
        SingletonWithThread() = default;

        /**
         * @brief Destructor of the Singleton object can only be accessed by the derived class
         *
         */
        virtual ~SingletonWithThread() = default;

    private:
        static inline T* _instance = nullptr;             ///< derived class instance
        static inline std::once_flag _init_flag;          ///< initialization flag
        static inline std::once_flag _shutdown_flag;      ///< shutdown flag
        std::thread _thread;                              ///< thread ran by the object
        std::atomic<bool> _shutting_down = false;         ///< atomic flag to stop the thread execution

        /**
         * @brief Object creation function
         *
         */
        static void _init() {
            if (_instance == nullptr) {
                _instance = new T();
            }
        }

        /**
         * @brief Object cleanup function
         *
         */
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
