#pragma once

#include <chrono>
#include <common/circular_buffer.hh>

namespace springtail {

    /** Type to measure the event frequency. WindowSize is the max
     * number of events to avearge over.
     */
    template<int WindowSize>
    struct EventFrequency {
        static_assert(WindowSize >= 2);

        using clock = std::chrono::steady_clock;

        EventFrequency() : _time_intervals(WindowSize)
        {}

        /** New event */
        void event()
        {
            auto now = clock::now();
            if (_last_event.time_since_epoch().count() != 0) {
                _time_intervals.put(now - _last_event);
                if (_time_intervals.size() > WindowSize)
                    // will pop the first interval
                    _time_intervals.next();
            }
            _last_event = now;
        }

        /** Returns the instant frequency in Hz */
        double frequency() const {
            if (_time_intervals.empty()) {
                return 0.0;
            }

            double sum = 0;

            for (size_t i = 0; i != _time_intervals.size(); ++i) {
                sum += std::chrono::duration<double>(_time_intervals[i]).count();
            }

            double average_interval = sum / static_cast<double>(_time_intervals.size());
            
            if (average_interval <= std::numeric_limits<double>::min()) { // whatever it means
                return std::numeric_limits<double>::max();
            }

            return 1.0 / average_interval;
        }

    private:
        CircularBuffer<clock::duration> _time_intervals;
        clock::time_point _last_event;
    };
}
