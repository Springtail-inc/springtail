#pragma once

#include <signal.h>
#include <sys/socket.h>

#include <common/init.hh>

#include <ipc/io_watcher.hh>
#include <ipc/pid_watcher.hh>
#include <ipc/signal_watcher.hh>
#include <ipc/timer_watcher.hh>

#include <pg_fdw/pg_xid_collector_common.hh>

namespace springtail::pg_fdw {

    class PgXidCollector : public Singleton<PgXidCollector>
    {
        friend class Singleton<PgXidCollector>;
    public:
        static constexpr int        LOOP_MAX_EVENTS = 50;
        static constexpr int        LOOP_TIMEOUT_MSEC = 1'000;
        static constexpr uint64_t   REDIS_UPDATE_INTERVAL_MSEC = 5'000;

        class FdwPidWatcher : public ipc::PidEventWatcher
        {
        public:
            FdwPidWatcher(pid_t pid) : ipc::PidEventWatcher(pid) {}

            // use move constructor and operator
            FdwPidWatcher(FdwPidWatcher&&) noexcept = default;
            FdwPidWatcher& operator=(FdwPidWatcher&&) noexcept = default;

            // remove copy constructors and operators
            FdwPidWatcher(const FdwPidWatcher&) = delete;
            FdwPidWatcher& operator=(const FdwPidWatcher&) = delete;

            virtual void on_process_event() noexcept override
            {
                PgXidCollector::get_instance()->on_fdw_death(_process_id);
            }
        };

        class RedisUpdateTimer : public ipc::PeriodicTimerWatcher
        {
        public:
            RedisUpdateTimer() : ipc::PeriodicTimerWatcher(CLOCK_MONOTONIC, REDIS_UPDATE_INTERVAL_MSEC) {}
            virtual void on_timeout(uint64_t timeout_count) noexcept
            {
                PgXidCollector::get_instance()->update_and_clean();
            }
        };

        class TermSignalWatcher : public ipc::SignalEventWatcher
        {
        public:
            TermSignalWatcher() : ipc::SignalEventWatcher(std::vector<int>{SIGINT, SIGTERM, SIGQUIT, SIGUSR1, SIGUSR2}) {}
            virtual void on_signal(const struct signalfd_siginfo &signal) noexcept
            {
                LOG_INFO("Received signal: {}, Description: ", signal.ssi_signo, strsignal(signal.ssi_signo));
                PgXidCollector::get_instance()->on_terminate();
            }
        };

        class UnixSocketWatcher : public ipc::IOEventWatcher
        {
        public:
            static constexpr size_t     MAX_MSG_COUNT = 16;
            static constexpr size_t     MSG_SIZE = sizeof(PgXidCollectorMsg);
            static constexpr size_t     CONTROL_MSG_LEN = CMSG_SPACE(sizeof(struct ucred));

            UnixSocketWatcher(const std::string_view &socket_name) : ipc::IOEventWatcher()
            {

                _fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
                PCHECK(_fd != -1) << "Failed to create UNIX domain socket";

                // Enable SO_PASSCRED
                int enable = 1;
                CHECK(setsockopt(_fd, SOL_SOCKET, SO_PASSCRED, &enable, sizeof(enable)) == 0)
                    << "Failed to set SO_PASSCRED";

                struct sockaddr_un addr = {};
                addr.sun_family = AF_UNIX;

                // Abstract: starts with '\0', no trailing null
                addr.sun_path[0] = '\0';
                memcpy(addr.sun_path + 1, socket_name.data(), socket_name.size());

                size_t addr_len = offsetof(struct sockaddr_un, sun_path) + 1 + socket_name.size();
                PCHECK(bind(_fd, (struct sockaddr*)&addr, addr_len) == 0) << "Failed to bind UNIX domain socket to address";

                _owns_fd = true;
                modify_events(EventChangeSet, EventChangeNone);

                // initialize receiving data structures
                for (int i = 0; i < MAX_MSG_COUNT; ++i) {
                    // each iovec points to its own buffer
                    _iovecs[i].iov_base = _buffers[i];
                    _iovecs[i].iov_len = MSG_SIZE;

                    // each header contains 1 iovec
                    _msgs_hdr[i].msg_hdr.msg_iov = &_iovecs[i];
                    _msgs_hdr[i].msg_hdr.msg_iovlen = 1;

                    // each header contains 1 control struct
                    _msgs_hdr[i].msg_hdr.msg_control = _ctrls[i];
                    _msgs_hdr[i].msg_hdr.msg_controllen = CONTROL_MSG_LEN;
                }
            }

            virtual void on_io_event(EpollEventType event) noexcept override
            {
                CHECK(event == EpollEventRead) << "Received invalid event";

                int rc = ::recvmmsg(_fd, _msgs_hdr, MAX_MSG_COUNT, 0, NULL);
                PCHECK(rc > -1 || (errno == EWOULDBLOCK || errno == EAGAIN)) << "Unexpected error for recvmmsg() call";
                for (int i = 0; i < rc; ++i) {
                    int len = _msgs_hdr[i].msg_len;
                    DCHECK(len == MSG_SIZE) << "Unexpected message size";

                    PgXidCollectorMsg *msg = reinterpret_cast<PgXidCollectorMsg *>(_msgs_hdr[i].msg_hdr.msg_iov->iov_base);

                    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&_msgs_hdr[i].msg_hdr);
                    DCHECK(cmsg != nullptr && cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_CREDENTIALS) << "Unexpected control message";
                    struct ucred *cred = (struct ucred *)CMSG_DATA(cmsg);

                    PgXidCollector::get_instance()->on_update(cred->pid, msg->db_id, msg->xid);
                }
            }

            // Let it die for now, not sure what kind of errors to expect at the moment
            virtual void on_error(int error_number) noexcept override
            {
                CHECK(false) << "Received error: " << error_number;
            }
        protected:
            struct mmsghdr _msgs_hdr[MAX_MSG_COUNT]{};
            struct iovec _iovecs[MAX_MSG_COUNT]{};
            char _buffers[MAX_MSG_COUNT][MSG_SIZE]{};
            char _ctrls[MAX_MSG_COUNT][CONTROL_MSG_LEN]{};
        };

        void run() noexcept;

        void on_terminate() { _loop->stop(); }

        void on_fdw_death(pid_t pid) noexcept;

        void update_and_clean() noexcept;

        void on_update(pid_t process_id, uint64_t db_id, uint64_t xid) noexcept;

    protected:
        PgXidCollector() noexcept;
        virtual ~PgXidCollector() noexcept override;

        std::shared_ptr<ipc::EventLoop> _loop;
        std::map<pid_t, std::shared_ptr<FdwPidWatcher>> _active_fdws;
        std::map<pid_t, std::shared_ptr<FdwPidWatcher>> _dead_fdws;
        // mapping from database id to a pair of fdw process id and xid it uses
        std::map<uint64_t, std::map<pid_t, uint64_t>> _db_pid_to_xid;
        std::map<pid_t, std::pair<uint64_t, uint64_t>> _pid_to_db_xid;

        // define event driven classes
        RedisUpdateTimer _redis_update_timer;
        TermSignalWatcher _term_signal_watcher;
        std::unique_ptr<UnixSocketWatcher> _unix_socket_watcher;
        std::string _fdw_id;
        std::string _socket_name;
    };

} // springtail::pg_fdw