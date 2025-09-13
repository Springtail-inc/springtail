#pragma once

#include <iostream>

#include <atomic>
#include <mutex>
#include <thread>

#include <absl/log/check.h>

namespace springtail {

    enum class ServiceId: int32_t
    {
        ServiceInvalidId = -1,
        ServiceRegisterId = 0,
        DatabaseMgrId,
        UserMgrId,
        ProxyServerId,
        XidMgrServerId,
        XidMgrClientId,
        SysTblMgrServerId,
        SysTblMgrClientId,
        WriteCacheServerId,
        WriteCacheClientId,
        IOMgrId,
        SchemaMgrId,
        TableMgrId,
        SyncTrackerId,
        PgFdwMgrId,
        PgXidSubscriberMgrId,
        PgDDLMgrId,
        PgLogCoordinatorId,
        StorageCacheId,
        VacuumerId,
        SystemTableMgrId,
        ServiceCountId
    };

    using ShutdownFunc = void(*)();

    void springtail_register_service(ServiceId service_id, ShutdownFunc fn);
    const std::string &springtail_get_service_name(ServiceId id);

    template <typename T>
    std::string get_type_name() {
        std::string func = __PRETTY_FUNCTION__;
        std::cout << "---> " << func << std::endl;
        auto start = func.find("T = ") + 4;
        auto end = func.find(';', start);
        return func.substr(start, end - start);
    }

    template <typename T>
    class Singleton {
    public:
        /**
         * @brief Get the instance template object. Calls _init() that creates the object only once
         *
         * @return the pointer to the derived class T
         */
        static T *get_instance()
        {
            std::call_once(_init_flag, _init);
            assert(_instance);
            return _instance;
        }

        // Copy constructor and assignment operator are deleted
        Singleton(const Singleton&) = delete;
        Singleton& operator=(const Singleton&) = delete;

        // Move constructor and move operator are deleted
        Singleton(Singleton&&) = delete;
        Singleton& operator=(Singleton&&) = delete;

        /**
         * @brief Start the thread
         *
         */
        void start_thread()
        {
            _has_thread = true;
            _thread = std::thread(&T::_internal_run, (T *)this);
            std::string name = springtail_get_service_name(_service_id);
            if (name == "Invalid") {
                name = get_type_name<T>();
                if (name.length() > 15) {
                    name = name.substr(0, 15);
                }
            }
            pthread_setname_np(_thread.native_handle(), name.c_str());
        }

        /**
         * @brief Shutdown function will only perform shutdown once
         *
         */
        static void shutdown()
        {
            std::call_once(_shutdown_flag, _shutdown);
        }

    protected:
        /**
         * @brief This function is intended to be provided by the derived class to perform
         *          whatever cleanup is necessary before the thread is joined.
         *          For example, if the thread is waiting on a conditional variable,
         *          this is where you would call notify_all() on this conditional
         *          variable to get the thread to wake up.
         *
         */
        virtual void _internal_thread_shutdown() {}

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
         * @brief Constructor of a new Singleton object can only be accessed by the derived class
         *
         */
        explicit Singleton(ServiceId service_id = ServiceId::ServiceInvalidId) : _service_id(service_id)
        {
            DCHECK(service_id >= ServiceId::ServiceInvalidId && service_id < ServiceId::ServiceCountId);
            if (service_id > ServiceId::ServiceInvalidId) {
                springtail_register_service(service_id, T::shutdown);
            }
        }

        /**
         * @brief Destructor of the Singleton object can only be accessed by the derived class
         *
         */
        virtual ~Singleton() = default;

        /**
         * @brief Assert if the singleton object has not been created yet
         *
         */
        static void _assert_instance()
        {
            CHECK_NE(_instance, nullptr);
        }

        /**
         * @brief Verify that a singleton instance was created
         *
         * @return true
         * @return false
         */
        static bool _has_instance()
        {
            return _instance != nullptr;
        }

    private:
        static inline T* _instance = nullptr;             ///< derived class instance
        static inline std::once_flag _init_flag;          ///< initialization flag
        static inline std::once_flag _shutdown_flag;      ///< shutdown flag
        std::thread _thread;                              ///< thread ran by the object
        ServiceId _service_id;                            ///< class service id
        bool _has_thread{false};                          ///< singleton with thread
        std::atomic<bool> _shutting_down{false};       ///< atomic flag to stop the thread execution

        /**
         * @brief Object creation function
         *
         */
        static void _init()
        {
            if (_instance == nullptr) {
                _instance = new T();
            }
        }

        /**
         * @brief Object cleanup function
         *
         */
        static void _shutdown()
        {
            if (_instance != nullptr) {
                _instance->_shutting_down = true;
                if (_instance->_has_thread) {
                    _instance->_internal_thread_shutdown();
                    _instance->_thread.join();
                }
                _instance->_internal_shutdown();
                delete _instance;
                _instance = nullptr;
            }
        }
    };

};
