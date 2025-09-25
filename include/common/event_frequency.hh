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

        EventFrequency() : _time_intervals{WindowSize}
        {}

        /** New event */
        void event()
        {
            ++_event_count;
            auto now = clock::now();
            if (_last_event != clock::time_point{}) {
                if (_time_intervals.size() ==  WindowSize) {
                    // will pop the first interval
                    _time_intervals.next();
                }
                _time_intervals.put(now - _last_event);
            }
            _last_event = now;
        }

        /** Returns the instant frequency in Hz */
        double frequency() const 
        {
            if (_time_intervals.empty()) {
                return 0.0;
            }

            // throttle the measurements
            if (_event_count < WindowSize/2) {
                return _last_freq;
            }

            double sum = 0;

            for (size_t i = 0; i != _time_intervals.size(); ++i) {
                sum += std::chrono::duration<double>(_time_intervals[i]).count();
            }

            double average_interval = sum / static_cast<double>(_time_intervals.size());
            
            if (average_interval <= std::numeric_limits<double>::min()) { // whatever it means
                return std::numeric_limits<double>::max();
            }

            _last_freq = 1.0 / average_interval;
            _event_count = 0;

            return _last_freq;
        }

    private:
        CircularBuffer<clock::duration> _time_intervals;
        clock::time_point _last_event;
        mutable size_t _event_count = 0;
        mutable double _last_freq = 0.0;
    };
}
