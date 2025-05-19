#pragma once

#include <common/singleton.hh>
#include <common/redis.hh>
#include <atomic>
#include <unordered_map>
#include <thread>
#include <mutex>

namespace springtail {
    /**
     * Singleton coordinator client to provide interface
     * for daemons to register provide liveness information
     * that is stored in redis and used by the coordinator
     */
    class Coordinator : public SingletonWithThread<Coordinator> {
        friend class SingletonWithThread<Coordinator>;
    public:
        /**
         * Daemon type
         * NOTE: if updating this, must update LivenessDaemonType in scheduler.py
         */
        enum DaemonType : uint8_t {
            LOG_MGR=1,
            WRITE_CACHE=2,
            XID_MGR=3,
            DDL_MGR=4,
            GC_MGR=5,
            SYS_TBL_MGR=6,
            PROXY=7,
            FDW=8,
            XID_SUBSCRIBER=9
        };

        /**
         * @brief Helper function to update a thread's timestamp
         * @param timestamp Reference to the thread's atomic timestamp
         */
        static void mark_alive(std::atomic<uint64_t>& timestamp) {
            auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            timestamp.store(epoch_ms);
        }

        /**
         * @brief Register a thread with the coordinator
         * @param type daemon type
         * @param thread_id thread id
         * @return Reference to atomic timestamp that the thread should update
         */
        std::atomic<uint64_t>& register_thread(DaemonType type, const std::string &thread_id="0");

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
         * @brief Kill a daemon; mark it as dead, notify the coordinator
         * @param type daemon type
         * @param thread_id thread id
         */
        void kill_daemon(DaemonType type, const std::string &thread_id="0");

    protected:
        /** Private constructor */
        Coordinator();
        /** Private destructor */
        ~Coordinator();

        /**
         * @brief Internal shutdown function
         */
        void _internal_shutdown() override;

        /**
         * @brief Internal run function for the thread
         */
        void _internal_run() override;

    private:
        static constexpr std::chrono::seconds BACKGROUND_THREAD_SLEEP_DURATION{1};  // Sleep duration for background thread

        uint64_t _db_instance_id;             // db instance id
        std::mutex _threads_mutex;            // mutex for thread map access

        // Map of (type,thread_id) -> atomic timestamp
        std::unordered_map<std::string, std::atomic<uint64_t>> _thread_timestamps;
    };
}
