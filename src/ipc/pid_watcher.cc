#include <sys/pidfd.h>
#include <sys/syscall.h>

#include <ipc/pid_watcher.hh>

namespace springtail::ipc {

    PidEventWatcher::PidEventWatcher(pid_t process_id) noexcept:
        EventWatcher("Process Id Watcher"), _process_id(process_id)
    {
        _event.events = (EPOLLIN | EPOLLRDHUP | EPOLLET);
    }

    void PidEventWatcher::start(std::shared_ptr<EventLoop> loop) noexcept
    {
        if (_fd == -1) {
            _fd = ::syscall(SYS_pidfd_open, _process_id, PIDFD_NONBLOCK);
            PCHECK(_fd !=  -1) <<  "Call to pidfd_open() failed";
        }
        EventWatcher::start(loop);
    }

    void PidEventWatcher::stop() noexcept
    {
        if (_loop) {
            EventWatcher::stop();
            ::close(_fd);
            _fd = -1;
        }
    }

    void PidEventWatcher::on_events(uint32_t events) noexcept
    {
        if (events & EPOLLIN) {
            on_process_event();
            stop();
        }
    }
} // springtail::ipc