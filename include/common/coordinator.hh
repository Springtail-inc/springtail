#pragma once

#include <common/singleton.hh>
#include <common/redis.hh>

namespace springtail {
    /**
     * Singleton coordinator client to provide interface
     * for daemons to register provide liveness information
     * that is stored in redis and used by the coordinator
     */
    class Coordinator : public Singleton<Coordinator> {
        friend class Singleton<Coordinator>;
    public:
        /** Daemon type */
        enum DaemonType : uint8_t {
            LOG_MGR=1,
            WRITE_CACHE=2,
            XID_MGR=3,
            DDL_MGR=4,
            GC_MGR=5,
            SYS_TBL_MGR=6,
            PROXY=7,
            FDW=8
        };

        /**
         * @brief Register a thread with the coordinator
         * @param type daemon type
         * @param thread_id thread id
         */
        void register_thread(DaemonType type, const std::string &thread_id="0");

        /**
         * @brief Unregister a thread with the coordinator
         * @param type daemon type
         * @param thread_id thread id
         */
        void unregister_thread(DaemonType type, const std::string &thread_id="0");

        /**
         * @brief Unregister a set of threads for a given daemon
         * @param type daemon type
         * @param threads a list of thread ids
         */
        void unregister_threads(DaemonType type, const std::vector<std::string> &threads);

        /**
         * @brief Get the list of registered threads for a given daemon
         * @param type daemon type
         * @return A list of thread ids
         */
        std::vector<std::string> get_threads(DaemonType type);

        /**
         * @brief Mark a daemon as alive; refresh it's timestamp
         * @param type daemon type
         * @param thread_id thread id
         */
        void mark_alive(DaemonType type, const std::string &thread_id="0");

        /**
         * @brief Kill a daemon; mark it as dead, notify the coordinator
         * @param type daemon type
         * @param thread_id thread id
         */
        void kill_daemon(DaemonType type, const std::string &thread_id="0");

    private:
        /** Private constructor */
        Coordinator();
        /** Private destructor */
        ~Coordinator() = default;

        uint64_t _db_instance_id;             // db instance id

        /**
         * @brief Internally set liveness status for daemon
         * @param type daemon type
         * @param thread_id thread id
         * @param alive true if alive, false if dead
         */
        void _set_liveness(DaemonType type, const std::string &thread_id, bool alive);
    };
}
