#pragma once

#include <cstdint>
#include <map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>

#include <absl/log/check.h>

namespace springtail::committer {

    /**
    * @brief Notification object for table copy operations.
    *
    * TableCopyNotify manages a set of table OIDs (toids) that are being tracked for copy table completion.
    * It provides thread-safe methods to set the tracked OIDs, wait for their completion,
    * notify when OIDs are completed, and check if all OIDs are done.
    */
    class TableCopyNotify
    {
    private:
        std::unordered_set<uint32_t> _toids;           // Set of table OIDs being tracked
        mutable std::mutex _toids_mutex;               // Mutex for thread-safe access to _toids
        std::condition_variable_any _toid_cv;          // Condition variable for notification

    public:

        /**
        * @brief Set the table OIDs to track.
        *
        * @param new_toids Set of new table OIDs to track.
        *
        * This method should only be called when no OIDs are currently being tracked.
        */
        void set_toids(const std::unordered_set<uint32_t> &new_toids)
        {
            std::unique_lock lock(_toids_mutex);
            DCHECK(_toids.empty());
            _toids = new_toids;
        }

        /**
        * @brief Wait until all tracked table OIDs are completed or shutdown is requested.
        *
        * @param shutdown Atomic flag to signal shutdown.
        */
        void wait(const std::atomic<bool> &shutdown)
        {
            std::unique_lock lock(_toids_mutex);
            _toid_cv.wait(lock, [this, &shutdown] { return _toids.empty() || shutdown.load(); });
        }

        /**
        * @brief Notify completion of specific table OIDs.
        *
        * @param completed_toids Vector of completed table OIDs.
        *
        * Removes completed OIDs from the tracked set and notifies waiting threads if all are done.
        */
        void notify(const std::vector<uint32_t> &completed_toids)
        {
            std::unique_lock lock(_toids_mutex);
            if (_toids.empty()) {
                return;
            }
            for (auto toid: completed_toids) {
                _toids.erase(toid);
            }
            if (_toids.empty()) {
                _toid_cv.notify_one();
            }
        }

        /**
        * @brief Check if all tracked table OIDs are completed.
        *
        * @return true if no OIDs are being tracked, false otherwise.
        */
        bool is_toids_empty() const
        {
            std::unique_lock lock(_toids_mutex);
            return _toids.empty();
        }
    };

    /**
    * @brief Tracker for table copy notifications per database.
    *
    * TableCopyTracker manages TableCopyNotify objects for each database ID.
    * It provides thread-safe methods to set tracked OIDs, wait for completion,
    * notify completion, check status, and remove tracking for a database.
    */
    class TableCopyTracker {
    private:
        std::map<uint64_t, TableCopyNotify> _table_sync_notify;   // Map of db_id to TableCopyNotify
        std::shared_mutex _table_sync_notify_mutex;               // Mutex for thread-safe access to map

    public:
        /**
        * @brief Wait for all table copies to complete for a database or until shutdown.
        *
        * @param db_id Database ID.
        * @param shutdown Atomic flag to signal shutdown.
        */
        void wait(uint64_t db_id, const std::atomic<bool> &shutdown)
        {
            std::shared_lock lock(_table_sync_notify_mutex);
            auto it = _table_sync_notify.find(db_id);
            DCHECK(it != _table_sync_notify.end());
            TableCopyNotify &notify_object = it->second;
            lock.unlock();
            notify_object.wait(shutdown);
        }

        /**
        * @brief Notify completion of table copies for a database.
        *
        * @param db_id Database ID.
        * @param completed_toids Vector of completed table OIDs.
        */
        void notify(uint64_t db_id, const std::vector<uint32_t> &completed_toids)
        {
            std::shared_lock lock(_table_sync_notify_mutex);
            auto it = _table_sync_notify.find(db_id);
            // if database is not found, then no one is waiting for notification
            if (it == _table_sync_notify.end()) {
                return;
            }
            TableCopyNotify &notify_object = it->second;
            lock.unlock();
            notify_object.notify(completed_toids);
        }

        /**
        * @brief Set the table OIDs to track for a database.
        *
        * @param db_id Database ID.
        * @param new_toids Set of new table OIDs to track.
        *
        * Creates a new TableCopyNotify object if one does not exist for the database.
        */
        void set_toids(uint64_t db_id, const std::unordered_set<uint32_t> &new_toids)
        {
            std::unique_lock lock(_table_sync_notify_mutex);
            auto it = _table_sync_notify.find(db_id);
            if (it == _table_sync_notify.end()) {
                it = _table_sync_notify.try_emplace(db_id).first;
                DCHECK(it != _table_sync_notify.end());
            }
            TableCopyNotify &notify_object = it->second;
            lock.unlock();
            notify_object.set_toids(new_toids);
        }

        /**
        * @brief Check if all tracked table OIDs are completed for a database.
        *
        * @param db_id Database ID.
        * @return true if no OIDs are being tracked, false otherwise.
        */
        bool is_toids_empty(uint64_t db_id)
        {
            std::shared_lock lock(_table_sync_notify_mutex);
            auto it = _table_sync_notify.find(db_id);
            DCHECK(it != _table_sync_notify.end());
            const TableCopyNotify &notify_object = it->second;
            lock.unlock();
            return notify_object.is_toids_empty();
        }

        /**
        * @brief Remove tracking for a database.
        *
        * @param db_id Database ID.
        */
        void remove_db(uint64_t db_id)
        {
            std::unique_lock write_lock(_table_sync_notify_mutex);
            _table_sync_notify.erase(db_id);
        }
    };

    using TableCopyTrackerPtr = std::shared_ptr<TableCopyTracker>;
} // springtail::committer
