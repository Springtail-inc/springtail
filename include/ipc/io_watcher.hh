#pragma once

#include <ipc/watcher.hh>

namespace springtail::ipc {

    class IOEventWatcher : public EventWatcher {
    public:
        enum EpollEventType {
            EpollEventNone,
            EpollEventRead,
            EpollEventWrite,
            EpollEventError,
            EpollEventReadHungUp,
            EpollEventHungUp
        };

        enum EventChangeType {
            EventChangeNone,    // do nothing
            EventChangeSet,     // set event
            EventChangeUnset    // unset event
        };

        IOEventWatcher() noexcept;
        virtual ~IOEventWatcher() noexcept;
        void set_fd(int fd, bool owns_fd = false) noexcept;

        // NOTE: if the watcher was previously started, it will be restarted or stoped.
        //  But it was never started, it will remain so till explicitly requested.
        void modify_events(EventChangeType read_event, EventChangeType write_event) noexcept;

        virtual void on_events(uint32_t events) noexcept override;
        virtual void on_io_event(EpollEventType event) noexcept = 0;
        virtual void on_error(int error_number) noexcept = 0;

    protected:
        bool _owns_fd{false};
    };

} // springtail::ipc
