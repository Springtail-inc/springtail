#pragma once

#include <sys/epoll.h>

#include <absl/log/check.h>

namespace springtail::ipc {
    class EventWatcher;

    class EventLoop {
    public:
        EventLoop(int max_events, int timeout) noexcept;

        void add_watcher(EventWatcher *watcher) noexcept;
        void remove_watcher(EventWatcher *watcher) noexcept;
        void modify_watcher(EventWatcher *watcher) noexcept;
        void run(bool once = false) noexcept;
        void remove_pending(int32_t pos) noexcept;
        void stop() noexcept;
        ~EventLoop() noexcept;

    private:
        struct epoll_event *_received_events;
        int _max_events;
        int _timeout;
        int _fd{-1};
        std::atomic<bool> _stop{false};

    };

} // springtail::ipc