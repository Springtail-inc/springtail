#include <absl/log/check.h>

#include <ipc/event_loop.hh>
#include <ipc/watcher.hh>

namespace springtail::ipc {
    EventLoop::EventLoop(int max_events, int timeout) noexcept : _max_events(max_events), _timeout(timeout) {
        CHECK(_max_events > 0) << "Parameter max_events should be greater than 0";
        _fd = epoll_create1(0);
        PCHECK(_fd !=  -1) << "epoll_create1(0) failed";
        _received_events = std::make_unique<struct epoll_event []>(static_cast<uint32_t>(_max_events));
        // _received_events = new struct epoll_event [static_cast<uint32_t>(_max_events)];
    }

    void EventLoop::add_watcher(EventWatcher *watcher) noexcept
    {
        DCHECK(_fd !=  -1) << "Invalid file descriptor";
        PCHECK(::epoll_ctl(_fd, EPOLL_CTL_ADD, watcher->_fd, &(watcher->_event)) != -1) << "Failed to add watcher";
    }

    void EventLoop::remove_watcher(EventWatcher *watcher) noexcept
    {
        DCHECK(_fd !=  -1) << "Invalid file descriptor";
        PCHECK(::epoll_ctl(_fd, EPOLL_CTL_DEL, watcher->_fd, &(watcher->_event)) != -1) << "Failed to remove watcher";
    }

    void EventLoop::modify_watcher(EventWatcher *watcher) noexcept
    {
        DCHECK(_fd !=  -1) <<  "Invalid file descriptor";
        PCHECK(::epoll_ctl(_fd, EPOLL_CTL_MOD, watcher->_fd, &(watcher->_event)) != -1) << "Failed to modify watcher";
    }

    void EventLoop::run(bool once) noexcept
    {
        DCHECK(_fd !=  -1) << "Invalid file descriptor";
        _stop = false;
        while (!_stop.load()) {
            // timeout argument: -1 - wait till an event is received,
            //                    0 - return immediately even if there is no events
            //                   >0 - the number of milliseconds to wait before returning if no events are present
            int event_count = ::epoll_wait(_fd, _received_events.get(), _max_events, (once)? -1: _timeout);

            if (event_count > 0) {
                for (int event_id = 0; event_id < event_count; ++event_id) {
                    EventWatcher *watcher = reinterpret_cast<EventWatcher *>(_received_events[event_id].data.ptr);
                    watcher->_pending_event_pos = event_id;
                }
                for (int event_id = 0; event_id < event_count; ++event_id) {
                    EventWatcher *watcher = reinterpret_cast<EventWatcher *>(_received_events[event_id].data.ptr);
                    if (watcher != nullptr) {
                        watcher->on_events(_received_events[event_id].events);
                        watcher->_pending_event_pos = -1;
                    }
                }
            } else {
                if (event_count == -1) {
                    PCHECK(errno ==  EINTR) << "epoll_wait() failed";
                    continue;
                }
            }
            if (once) {
                break;
            }
        }
    }

    void EventLoop::remove_pending(int32_t pos) noexcept
    {
        if (pos > -1) {
            _received_events[pos].data.ptr = nullptr;
        }
    }

    void EventLoop::stop() noexcept
    {
        _stop = true;
    }

    EventLoop::~EventLoop() noexcept
    {
        ::close(_fd);
        // delete [] _received_events;
    }

} // springtail::ipc
