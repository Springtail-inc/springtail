#include <sys/timerfd.h>

#include <absl/log/check.h>

#include <ipc/timer_watcher.hh>

namespace springtail::ipc {

    uint64_t TimerWatcher::get_time_in_msec(int clock_id)
    {
        struct timespec time_value{};
        int ret = clock_gettime(clock_id, &time_value);
        PCHECK(ret == 0) << "Function clock_gettime() failed";
        return static_cast<uint64_t>(time_value.tv_nsec) / NSEC_IN_MSEC + MSEC_IN_SEC * static_cast<uint64_t>(time_value.tv_sec);
    }

    TimerWatcher::TimerWatcher(int clock_id, uint64_t first_timeout, uint64_t interval, bool absolute, bool cancel_on_set) :
        EventWatcher("Timer Watcher"),
        _first_timeout(first_timeout),
        _interval(interval),
        _clock_id(clock_id),
        _periodic((interval != 0)),
        _absolute(absolute),
        _cancel_on_set(cancel_on_set)
    {
        _event.events = (EPOLLIN | EPOLLRDHUP | EPOLLET);
        CHECK(cancel_on_set == false || (clock_id == CLOCK_REALTIME && absolute == true)) << "Cancel On Set flag can only be set for absolute real time clock";
        _fd = timerfd_create(clock_id, (TFD_NONBLOCK | TFD_CLOEXEC));
        PCHECK(_fd != -1) << "Failed to create timer";
    }

    void TimerWatcher::set_next_timeout(uint64_t timeout)
    {
        CHECK(!_periodic) << "Can't change timeout for periodic timer";
        _first_timeout = timeout;
        _set_timeout();
    }

    void TimerWatcher::start(std::shared_ptr<EventLoop> loop) noexcept
    {
        EventWatcher::stop();
        _set_timeout();
        EventWatcher::start(loop);
    }

    void TimerWatcher::stop() noexcept
    {
        if (_loop != nullptr) {
            EventWatcher::stop();
            const struct itimerspec null_timeout{};
            int ret = timerfd_settime(_fd, 0, &null_timeout, nullptr);
            PCHECK(ret == 0|| (ret == -1 && _cancel_on_set && errno == ECANCELED)) << "Function timerfd_settime() failed";
        }
    }

    void TimerWatcher::on_events(uint32_t events) noexcept
    {
        if (events & EPOLLIN) {
            uint64_t timeout_count = 0;
            ssize_t ret = 0;
            while((ret = ::read(_fd, &timeout_count, sizeof(uint64_t))) == sizeof(uint64_t)) {
                on_timeout(timeout_count);
            }
            if (ret == 0) {
                CHECK(_clock_id == CLOCK_REALTIME && _absolute && !_cancel_on_set) << "Read unexpectedly returned 0 after reading from timer FD";
            } else if (ret == -1) {
                PCHECK((errno == EAGAIN || (!(_absolute && _cancel_on_set) || errno == ECANCELED))) << "Failed read()";
            } else {
                CHECK(false) << "Unexepected return value from timer FD";
            }
        }
    }

    uint64_t TimerWatcher::time_left() noexcept
    {
        if (_loop != nullptr) {
            struct itimerspec next_expiration_time{};
            int ret = ::timerfd_gettime(_fd, &next_expiration_time);
            PCHECK(ret == 0) << "Function call to timerfd_gettime() failed";
            return static_cast<uint64_t>(next_expiration_time.it_value.tv_sec) * MSEC_IN_SEC +
                    static_cast<uint64_t>(next_expiration_time.it_value.tv_nsec) / NSEC_IN_MSEC;
        }
        return 0;
    }

    TimerWatcher::~TimerWatcher() noexcept
    {
        stop();
    }

    void TimerWatcher::_set_timeout() noexcept
    {
        const struct itimerspec timeout = {
            {
                static_cast<__time_t>(_interval / MSEC_IN_SEC),
                static_cast<__syscall_slong_t>((_interval % MSEC_IN_SEC) * NSEC_IN_MSEC)
            },
            {
                static_cast<__time_t>(_first_timeout / MSEC_IN_SEC),
                static_cast<__syscall_slong_t>((_first_timeout % MSEC_IN_SEC) * NSEC_IN_MSEC)
            }
        };
        int flags = ((_absolute)? TFD_TIMER_ABSTIME : 0) | ((_cancel_on_set)? TFD_TIMER_CANCEL_ON_SET : 0);
        int ret = ::timerfd_settime(_fd, flags, &timeout, nullptr);
        PCHECK(ret == 0 || (ret == -1 && _absolute && _cancel_on_set && errno == ECANCELED)) << "Function timerfd_settime() failed";
    }
} // springtail::ipc

