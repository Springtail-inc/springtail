#include <signal.h>

#include <absl/log/check.h>

#include <ipc/signal_watcher.hh>

namespace springtail::ipc {

    SignalEventWatcher::SignalEventWatcher(sigset_t mask) noexcept :
        EventWatcher("Signal Watcher"), _signal_mask(mask)
    {
        _event.events = (EPOLLIN | EPOLLRDHUP | EPOLLET);
    }

    SignalEventWatcher::SignalEventWatcher(const std::vector<int> &signal_list) noexcept :
        EventWatcher("Signal Watcher")
    {
        size_t signal_count = signal_list.size();
        CHECK(signal_count > 0) << "Signal list is empty";

        // set signal mask from vector
        sigemptyset(&_signal_mask);
        for (auto &signo: signal_list) {
            sigaddset(&_signal_mask, signo);
        }

        _event.events = (EPOLLIN | EPOLLRDHUP | EPOLLET);

    }

    void SignalEventWatcher::start(std::shared_ptr<EventLoop> loop) noexcept
    {
        if (_fd == -1) {
            PCHECK(sigprocmask(SIG_BLOCK, &_signal_mask, nullptr) == 0) << "Call to sigprocmask() failed";
            _fd = signalfd(-1, &_signal_mask, (SFD_NONBLOCK | SFD_CLOEXEC));
            PCHECK(_fd !=  -1) <<  "Call to signalfd() failed";
        }
        EventWatcher::start(loop);
    }

    void SignalEventWatcher::on_events(uint32_t events) noexcept
    {
        struct signalfd_siginfo signal_info{};
        if (events & EPOLLIN) {
            ssize_t read_size = 0;
            while ((read_size = read(_fd, &signal_info, sizeof(struct signalfd_siginfo))) == sizeof(struct signalfd_siginfo)) {
                PCHECK(sigismember(&(_signal_mask), static_cast<int>(signal_info.ssi_signo)) == 1) <<  "Call to sigismember() failed";
                on_signal(signal_info);
            }
            PCHECK(read_size != -1 || errno == EAGAIN) << "Call to read() failed";
        }
    }

    SignalEventWatcher::~SignalEventWatcher() noexcept
    {
        PCHECK(sigprocmask(SIG_UNBLOCK, &_signal_mask, nullptr) != -1) <<  "Call to sigprocmask() failed";
    }

} // springtail::ipc