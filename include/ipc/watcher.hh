#pragma once

#include <ipc/event_loop.hh>

namespace springtail::ipc {
    class EventWatcher {
        friend class EventLoop;
    public:
        explicit EventWatcher(const std::string &name) noexcept;
        virtual ~EventWatcher() noexcept;

        virtual void start(std::shared_ptr<EventLoop> loop) noexcept;
        virtual void stop() noexcept;
        virtual void restart() noexcept;

        virtual void on_events(uint32_t events) noexcept = 0;

    protected:
        std::string _name;
        std::shared_ptr<EventLoop> _loop{nullptr};
        struct epoll_event _event;
        int _fd{-1};
        int32_t _pending_event_pos{-1};
    };

} // springtail::ipc