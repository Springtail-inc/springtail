#pragma once

#include <ipc/watcher.hh>

namespace springtail::ipc {
    class TimerWatcher : public EventWatcher {
    public:
        static constexpr uint64_t NSEC_IN_SEC = 1'000'000'000;
        static constexpr uint64_t MSEC_IN_SEC = 1'000;
        static constexpr uint64_t NSEC_IN_MSEC = 1'000'000;

        static uint64_t get_time_in_msec(int clock_id);

        // clock_id can be one of those three {CLOCK_REALTIME, CLOCK_MONOTONIC, CLOCK_BOOTTIME}
        // type of the timer will be one time or periodic, which is determined by
        // first timeout and interval, both given in milliseconds
        // also it should be specified if we are using absolute time (TFD_TIMER_ABSTIME) and cancel on set (TFD_TIMER_CANCEL_ON_SET)
        // the last one is only valid for CLOCK_REALTIME timers when TFD_TIMER_ABSTIME is set
        // if absolute time is used, expiration time should be determined using clock_gettime() function
        // using the same clock_id as the timer
        TimerWatcher(int clock_id, uint64_t first_timeout, uint64_t interval, bool absolute = false, bool cancel_on_set = false);

        void set_next_timeout(uint64_t timeout);
        virtual void start(std::shared_ptr<EventLoop> loop) noexcept override;
        virtual void stop() noexcept override;

        virtual void on_timeout(uint64_t timeout_count) noexcept = 0;

        virtual void on_events(uint32_t events) noexcept override;
        virtual uint64_t time_left() noexcept;
        virtual ~TimerWatcher() noexcept override;

    protected:
        uint64_t _first_timeout{0};
        uint64_t _interval{0};
        int _clock_id{0};
        bool _periodic : 1 {false};
        bool _absolute : 1 {false};
        bool _cancel_on_set : 1 {false};

        virtual void _set_timeout() noexcept;
    };

    class OneTimeTimerWatcher : public TimerWatcher {
    public:
        OneTimeTimerWatcher(int clock_id, uint64_t timeout, bool abosolute = false, bool cancel_on_set = false) noexcept :
            TimerWatcher(clock_id, timeout, 0, abosolute, cancel_on_set)
        {
            _name = "OneTimeTimerWatcher";
        }
        virtual ~OneTimeTimerWatcher() noexcept = default;
    };

    class PeriodicTimerWatcher : public TimerWatcher {
    public:
        PeriodicTimerWatcher(int clock_id, uint64_t interval) noexcept :
            TimerWatcher(clock_id, interval, interval, false, false)
        {
            _name = "PeriodicTimerWatcher";
        }
        virtual ~PeriodicTimerWatcher() noexcept = default;
    };
} // springtail::ipc