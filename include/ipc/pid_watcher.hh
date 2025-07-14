#pragma once

#include <ipc/watcher.hh>

namespace springtail::ipc {

    class PidEventWatcher : public EventWatcher {
    public:
        explicit PidEventWatcher(pid_t process_id) noexcept;
        virtual ~PidEventWatcher() noexcept = default;

        PidEventWatcher(const PidEventWatcher&) = delete;
        PidEventWatcher& operator=(const PidEventWatcher&) = delete;
        PidEventWatcher(PidEventWatcher&&) = delete;
        PidEventWatcher& operator=(PidEventWatcher&&) = delete;

        virtual void on_process_event() noexcept = 0;

        virtual void start(std::shared_ptr<EventLoop> loop) noexcept override;
        virtual void stop() noexcept override;
        virtual void on_events(uint32_t events) noexcept override;

    protected:
        pid_t _process_id{0};
    };

} // springtail::ipc