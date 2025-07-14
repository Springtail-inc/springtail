#include <ipc/watcher.hh>

namespace springtail::ipc {
    EventWatcher::EventWatcher(const std::string &name) noexcept : _name(name)
    {
        _event.data.ptr = this;
    }

    void EventWatcher::start(std::shared_ptr<EventLoop> loop) noexcept
    {
        stop();
        _loop = loop;
        _loop->add_watcher(this);
    }

    void EventWatcher::stop() noexcept
    {
        if (_loop != nullptr) {
            _loop->remove_watcher(this);
            _loop.reset();
        }
    }

    void EventWatcher::restart() noexcept
    {
        if (_loop != nullptr) {
            _loop->modify_watcher(this);
        }
    }

    EventWatcher::~EventWatcher() noexcept
    {
        if (_loop != nullptr) {
            if (_pending_event_pos > -1) {
                _loop->remove_pending(_pending_event_pos);
            }
            EventWatcher::stop();
        }
        if (_fd > -1) {
            ::close(_fd);
            _fd = -1;
        }
    }

} // springtail::ipc