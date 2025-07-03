#pragma once

namespace springtail {
    /**
     * @brief A simple interval tree based on std::map for managing numeric ranges.
     * 
     * Intervals are merged automatically on insert. Subtract removes overlaps.
     * T must be a numeric type (e.g., uint64_t).
     */
    template <typename T>
    class IntervalTree {
    public:
        /**
         * @brief Inserts an interval [start, end].
         *        Automatically merges with existing overlapping intervals.
         */
        void insert(T start, T end)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (start >= end) return;
            auto it = intervals.lower_bound(start);

            // Merge with previous if overlapping
            if (it != intervals.begin()) {
                auto prev = std::prev(it);
                if (prev->second >= start) {
                    start = std::min(start, prev->first);
                    end = std::max(end, prev->second);
                    intervals.erase(prev);
                }
            }

            while (it != intervals.end() && it->first <= end) {
                end = std::max(end, it->second);
                it = intervals.erase(it);
            }

            intervals[start] = end;
        }

        /**
         * @brief Subtracts an interval [start, end] from the tree.
         *        Splits existing intervals if needed.
         */
        void subtract(T start, T end)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (start >= end) return;
            auto it = intervals.lower_bound(start);

            // Check and split overlapping previous interval
            if (it != intervals.begin()) {
                auto prev = std::prev(it);
                if (prev->second > start) {
                    T prev_start = prev->first;
                    T prev_end = prev->second;
                    intervals.erase(prev);
                    if (prev_start < start)
                        intervals[prev_start] = start;
                    if (prev_end > end)
                        intervals[end] = prev_end;
                }
            }

            // Remove or trim overlapping intervals
            while (it != intervals.end() && it->first < end) {
                T it_start = it->first;
                T it_end = it->second;
                it = intervals.erase(it);
                if (it_end > end)
                    intervals[end] = it_end;
            }
        }

        /**
         * @brief Returns the stored intervals as a sorted vector of [start, end] pairs.
         */
        std::vector<std::pair<T, T>> to_vector() const
        {
            std::lock_guard<std::mutex> lock(_mutex);
            std::vector<std::pair<T, T>> result;
            for (const auto& [start, end] : intervals)
                result.emplace_back(start, end);
            return result;
        }

        void clear()
        {
            std::lock_guard<std::mutex> lock(_mutex);
            intervals.clear();
        }

        bool empty() const
        {
            std::lock_guard<std::mutex> lock(_mutex);
            return intervals.empty();
        }

    private:
        std::map<T, T> intervals;
        mutable std::mutex _mutex;
    };
}
