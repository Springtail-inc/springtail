#pragma once

#include <mutex>
#include <memory>

#include <common/redis.hh>

namespace springtail {
    /**
     * Singleton coordinator client to provide interface
     * for daemons to register provide liveness information
     * that is stored in redis and used by the coordinator
     */
    class Coordinator {
    public:
        /** Daemon type */
        enum DaemonType : uint8_t {
            LOG_MGR=1,
            WRITE_CACHE,
            XID_MGR,
            DDL_MGR,
            GC_MGR,
            SYS_TBL_MGR,
            FDW
        };

        /**
         * @brief Get the instance object
         * @return CoordinatorClient*
         */
        static Coordinator* get_instance() {
            std::call_once(_init_flag, &Coordinator::_init);
            return _instance;
        }

        /**
         * @brief Register a thread with the coordinator
         * @param type daemon type
         * @param thread_id thread id
         */
        void register_thread(DaemonType type, uint64_t thread_id=0);

        /**
         * @brief Unregister a thread with the coordinator
         * @param type daemon type
         * @param thread_id thread id
         */
        void unregister_thread(DaemonType type, uint64_t thread_id=0);

        /**
         * @brief Set the thread liveness object
         * @param type daemon type
         * @param thread_id thread id
         */
        void set_liveness(DaemonType type, uint64_t thread_id=0);

        /**
         * @brief Kill a daemon; mark it as dead, notify the coordinator
         * @param type daemon type
         * @param thread_id thread id
         */
        void kill_daemon(DaemonType type, uint64_t thread_id=0);

    private:
        /** Private constructor */
        Coordinator();
        /** Private destructor */
        ~Coordinator() = default;

        /** Delete constructor */
        Coordinator(const Coordinator&) = delete;
        Coordinator& operator=(const Coordinator&) = delete;
        Coordinator(Coordinator&&) = delete;
        Coordinator& operator=(Coordinator&&) = delete;

        uint64_t _db_instance_id;             // db instance id

        static Coordinator* _instance;  // singleton instance
        static std::once_flag _init_flag;     // once init flag

        /**
         * @brief Initialize the singleton instance
         */
        static void _init();

        /**
         * @brief Internally set liveness status for daemon
         * @param type daemon type
         * @param thread_id thread id
         * @param alive true if alive, false if dead
         */
        void _set_liveness(DaemonType type, uint64_t thread_id, bool alive);
    };
}