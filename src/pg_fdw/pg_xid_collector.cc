#include <sys/epoll.h>
#include <sys/pidfd.h>

#include <chrono>

#include <common/constants.hh>
#include <common/json.hh>
#include <common/redis_types.hh>

#include <pg_fdw/pg_xid_collector.hh>

namespace springtail::pg_fdw {

    PgXidCollector::PgXidCollector() noexcept : Singleton<PgXidCollector>()
    {
        uint64_t db_instance_id = Properties::get_db_instance_id();
        _redis_hash_name = fmt::format(redis::HASH_MIN_XID, db_instance_id);
        _fdw_id = Properties::get_fdw_id();
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(_fdw_id);
        _socket_name = Json::get_or<std::string_view>(fdw_config, "collector_socket", DEFAULT_SOCKET_NAME);

        _redis_thread = std::jthread(&PgXidCollector::_redis_thread_run, this);
    }

    void PgXidCollector::_internal_run()
    {
        // create event loop
        int epoll_fd = ::epoll_create1(0);
        PCHECK(epoll_fd !=  -1) << "epoll_create1(0) failed";
        auto received_events = std::make_unique<struct epoll_event []>(static_cast<uint32_t>(LOOP_MAX_EVENTS));

        // open UNIX domain socket file descriptor
        int unix_fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        PCHECK(unix_fd != -1) << "Failed to create UNIX domain socket";

        // Enable SO_PASSCRED
        int enable = 1;
        CHECK(::setsockopt(unix_fd, SOL_SOCKET, SO_PASSCRED, &enable, sizeof(enable)) == 0)
            << "Failed to set SO_PASSCRED";

        struct sockaddr_un addr = {};
        addr.sun_family = AF_UNIX;

        // Abstract: starts with '\0', no trailing null
        addr.sun_path[0] = '\0';
        ::memcpy(addr.sun_path + 1, _socket_name.data(), _socket_name.size());

        size_t addr_len = offsetof(struct sockaddr_un, sun_path) + 1 + _socket_name.size();
        PCHECK(::bind(unix_fd, (struct sockaddr*)&addr, addr_len) == 0) << "Failed to bind UNIX domain socket to address";

        // initialize receiving data structures
        constexpr size_t max_msg_count = 16;
        constexpr size_t msg_size = sizeof(PgXidCollectorMsg);
        constexpr size_t control_msg_len = CMSG_SPACE(sizeof(struct ucred));

        struct mmsghdr msgs_hdr[max_msg_count]{};
        struct iovec iovecs[msg_size]{};
        char _buffers[max_msg_count][msg_size]{};
        char _ctrls[max_msg_count][control_msg_len]{};

        for (int i = 0; i < max_msg_count; ++i) {
            // each iovec points to its own buffer
            iovecs[i].iov_base = _buffers[i];
            iovecs[i].iov_len = msg_size;

            // each header contains 1 iovec
            msgs_hdr[i].msg_hdr.msg_iov = &iovecs[i];
            msgs_hdr[i].msg_hdr.msg_iovlen = 1;

            // each header contains 1 control struct
            msgs_hdr[i].msg_hdr.msg_control = _ctrls[i];
            msgs_hdr[i].msg_hdr.msg_controllen = control_msg_len;
        }

        // add file descriptor to epoll
        struct epoll_event unix_event = {.events = (EPOLLET | EPOLLIN), .data = {}};
        PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, unix_fd, &unix_event) != -1) << "Failed to register unix domain socket fd";

        // per process event storage
        std::map<pid_t, std::shared_ptr<struct pid_data>> pid_event_storage;

        // start running event loop
        while (!_is_shutting_down()) {
            int event_count = ::epoll_wait(epoll_fd, received_events.get(), LOOP_MAX_EVENTS, LOOP_TIMEOUT_MSEC);
            if (event_count > 0) {
                for (int event_id = 0; event_id < event_count; ++event_id) {
                    uint64_t id = received_events[event_id].data.u64;
                    uint32_t events = received_events[event_id].events;
                    if (id == 0) {
                        // this is unix_fd
                        CHECK(events & EPOLLIN) << "Received invalid event";

                        int rc = ::recvmmsg(unix_fd, msgs_hdr, max_msg_count, 0, nullptr);
                        PCHECK(rc > -1 || (errno == EWOULDBLOCK || errno == EAGAIN)) << "Unexpected error for recvmmsg() call";
                        for (int i = 0; i < rc; ++i) {
                            int len = msgs_hdr[i].msg_len;
                            DCHECK(len == msg_size) << "Unexpected message size";

                            PgXidCollectorMsg *msg = reinterpret_cast<PgXidCollectorMsg *>(msgs_hdr[i].msg_hdr.msg_iov->iov_base);

                            struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msgs_hdr[i].msg_hdr);
                            DCHECK(cmsg != nullptr && cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_CREDENTIALS) << "Unexpected control message";
                            struct ucred *cred = (struct ucred *)CMSG_DATA(cmsg);

                            if (!pid_event_storage.contains(cred->pid)) {
                                int process_fd = ::syscall(SYS_pidfd_open, cred->pid, PIDFD_NONBLOCK);
                                PCHECK(process_fd !=  -1) <<  "Call to pidfd_open() failed";

                                auto process_data = std::make_shared<struct pid_data>();
                                process_data->fd = process_fd;
                                process_data->event_data.events = (EPOLLET | EPOLLIN);
                                process_data->event_data.data.u64 = static_cast<uint64_t>(cred->pid);

                                PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, process_fd, &(process_data->event_data)) != -1)
                                    << "Failed to register process fd";
                                pid_event_storage.insert(std::make_pair(cred->pid, process_data));
                                LOG_DEBUG(LOG_FDW, "FDW process {} added, new process count {}", cred->pid, pid_event_storage.size());
                            }
                            on_update(cred->pid, msg->db_id, msg->xid);
                        }
                    } else {
                        // this is pid
                        pid_t pid = static_cast<pid_t>(id);
                        auto node = pid_event_storage.extract(pid);
                        CHECK(!node.empty());
                        auto process_data = std::move(node.mapped());
                        int process_fd = process_data->fd;
                        PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, process_fd, nullptr) != -1) << "Failed to remove process fd";
                        ::close(process_fd);
                        on_fdw_death(pid);
                        LOG_DEBUG(LOG_FDW, "FDW process {} died, remaining process count {}", pid, pid_event_storage.size());
                    }
                }
            } else {
                if (event_count == -1) {
                    PCHECK(errno == EINTR) << "epoll_wait() failed";
                    continue;
                }
            }
        }

        // when done
        // close UNIX domain socket file descriptor
        PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, unix_fd, nullptr) != -1) << "Failed to remove unix domain socket fd";
        ::close(unix_fd);

        // close all pid file discriptors (if we have any)
        for (auto &item: pid_event_storage) {
            PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, item.second->fd, nullptr) != -1) << "Failed to remove process fd";
            ::close(item.second->fd);
        }

        // close epoll file descriptor
        ::close(epoll_fd);
    }

    void PgXidCollector::on_update(pid_t process_id, uint64_t db_id, uint64_t xid) noexcept
    {
        LOG_DEBUG(LOG_FDW, "Updating xid: process {}, db_id {}, xid {}", process_id, db_id, xid);
        std::unique_lock lock(_data_mutex);
        if (_pid_to_db_id_xid.contains(process_id)) {
            auto [old_db_id, old_xid] = _pid_to_db_id_xid[process_id];
            uint64_t count = _db_id_to_xid_to_count[old_db_id][old_xid];
            CHECK(count > 0);
            --count;
            if (count != 0) {
                _db_id_to_xid_to_count[old_db_id][old_xid] = count;
            } else {
                _db_id_to_xid_to_count[old_db_id].erase(old_xid);
            }
        }
        _pid_to_db_id_xid.insert_or_assign(process_id, std::make_pair(db_id, xid));

        if (!_db_id_to_xid_to_count.contains(db_id)) {
            _db_id_to_xid_to_count.insert(std::make_pair(db_id, std::map<uint64_t, uint64_t>()));
        }
        if (!_db_id_to_xid_to_count[db_id].contains(xid)) {
            _db_id_to_xid_to_count[db_id].insert(std::make_pair(xid, 0));
        }
        uint64_t count = _db_id_to_xid_to_count[db_id][xid];
        count++;
        _db_id_to_xid_to_count[db_id][xid] = count;
    }

    void PgXidCollector::on_fdw_death(pid_t pid) noexcept
    {
        std::unique_lock lock(_data_mutex);
        CHECK(_pid_to_db_id_xid.contains(pid));
        auto [old_db_id, old_xid] = _pid_to_db_id_xid[pid];
        uint64_t count = _db_id_to_xid_to_count[old_db_id][old_xid];
        CHECK(count > 0);
        count--;
        LOG_DEBUG(LOG_FDW, "Removing process data: process {}, db_id {}, xid {}, remaining xid count {}",
            pid, old_db_id, old_xid, count);
        if (count != 0) {
            _db_id_to_xid_to_count[old_db_id][old_xid] = count;
        } else {
            _db_id_to_xid_to_count[old_db_id].erase(old_xid);
        }
        _pid_to_db_id_xid.erase(pid);
    }

    void PgXidCollector::_redis_thread_run(std::stop_token st)
    {
        std::chrono::milliseconds dur(REDIS_UPDATE_INTERVAL_MSEC);
        std::condition_variable_any cv;
        std::mutex m;
        while(!st.stop_requested()) {
            std::vector<std::pair<std::string, std::string>> db_xid_list;

            // collect min xids per database
            std::shared_lock data_lock(_data_mutex);
            for (auto &[db_id, xid_map]: _db_id_to_xid_to_count) {
                uint64_t xid = constant::LATEST_XID;
                if (!xid_map.empty()) {
                    xid = xid_map.begin()->first;
                }
                std::string key = fmt::format("{}:{}", _fdw_id, db_id);
                std::string value = std::to_string(xid);
                db_xid_list.push_back(std::make_pair(key, value));
                LOG_DEBUG( LOG_FDW,"Updating redis with key: {}, value: {}", key, value);
            }
            data_lock.unlock();

            // send data to redis
            if (!db_xid_list.empty()) {
                RedisClientPtr client = RedisMgr::get_instance()->get_client();
                client->hmset(_redis_hash_name, db_xid_list.begin(), db_xid_list.end());
            }

            std::unique_lock<std::mutex> lock(m);
            if (cv.wait_for(lock, dur, [&st] { return st.stop_requested(); })) {
                break;
            }
        }
    }
} // springtail::pg_fdw