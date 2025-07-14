#include <sys/socket.h>

#include <ipc/io_watcher.hh>

namespace springtail::ipc {
    IOEventWatcher::IOEventWatcher() noexcept : EventWatcher("IO Watcher")
    {
        _event.events = EPOLLET;
    }

    void IOEventWatcher::set_fd(int fd, bool owns_fd) noexcept
    {
        _fd = fd;
        _owns_fd = owns_fd;
    }

    void IOEventWatcher::modify_events(EventChangeType read_event, EventChangeType write_event) noexcept
    {
        CHECK((read_event != EventChangeNone || write_event != EventChangeNone)) << "No change specified for both read and write events";
        switch (read_event) {
            case EventChangeSet:
                _event.events |= (EPOLLIN | EPOLLRDHUP);
                break;
            case EventChangeUnset:
                _event.events &= ~(EPOLLIN |  EPOLLRDHUP);
                break;
            default:
                break;
        }
        switch (write_event) {
            case EventChangeSet:
                _event.events |= (EPOLLOUT);
                break;
            case EventChangeUnset:
                _event.events &= ~(EPOLLOUT);
                break;
            default:
                break;
        }
        if (_event.events == EPOLLET) {
            stop();
        } else{
            restart();
        }
    }

    void IOEventWatcher::on_events(uint32_t events) noexcept
    {
        if (events & EPOLLERR) {
            int error = 0;
            socklen_t errorSize = sizeof(error);
            PCHECK(::getsockopt(_fd, SOL_SOCKET, SO_ERROR, &error, &errorSize) == 0) << "Failed getsockopt() for the socket";
            on_error(error);
            events &= ~(EPOLLERR);
        }

        uint32_t flags[] = {EPOLLIN | EPOLLRDNORM, EPOLLOUT | EPOLLWRNORM, EPOLLRDHUP, EPOLLHUP};
        EpollEventType watcher_events[] =
            {EpollEventRead, EpollEventWrite, EpollEventReadHungUp, EpollEventHungUp};

        uint32_t i = 0;
        uint32_t max_i = sizeof(flags) / sizeof(flags[0]);
        while (events && i < max_i) {
            // check if the flag is set
            //  if it is set,  unset it and call handle_event()
            if (events & flags[i]) {
                events &= ~(flags[i]);
                on_io_event(watcher_events[i]);
            }
            ++i;
        }
    }

    IOEventWatcher::~IOEventWatcher() noexcept
    {
        //  prevent base class destructor from closing
        //  file descriptor as it does not own it
        if (!_owns_fd) {
            _fd = -1;
        }
    }

} // springtail::ipc