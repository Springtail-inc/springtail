#pragma once

#include <sys/signalfd.h>

#include <vector>

#include <ipc/watcher.hh>

namespace springtail::ipc {

    class SignalEventWatcher : public EventWatcher {
    public:
        explicit SignalEventWatcher(sigset_t mask) noexcept;
        explicit SignalEventWatcher(const std::vector<int> &signal_list) noexcept;

        virtual void on_signal(const struct signalfd_siginfo &signal) noexcept = 0;

        virtual void start(std::shared_ptr<EventLoop> loop) noexcept override;
        virtual void on_events(uint32_t events) noexcept override;

        virtual ~SignalEventWatcher() noexcept override;
    protected:
        sigset_t _signal_mask{0};
    };
} // springtail::ipc