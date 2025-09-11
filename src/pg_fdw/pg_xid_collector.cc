#include <sys/epoll.h>
#include <sys/pidfd.h>

#include <algorithm>
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
        _redis_pid_set_name = fmt::format(redis::SET_FDW_PID, db_instance_id);
        _fdw_id = Properties::get_fdw_id();
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(_fdw_id);
        _socket_name = Json::get_or<std::string_view>(fdw_config, "collector_socket", DEFAULT_SOCKET_NAME);
        _cleanup_redis_data();

        _redis_thread = std::thread(&PgXidCollector::_redis_thread_run, this);
    }

    void
    PgXidCollector::_cleanup_redis_data()
    {
        RedisClientPtr client = RedisMgr::get_instance()->get_client();
        unsigned long long cursor = 0;
        std::string pattern = fmt::format("{}:*", _fdw_id);

        std::vector<std::string> set_data;
        do {
            cursor = client->sscan(_redis_pid_set_name, cursor, pattern, std::back_inserter(set_data));
        } while(cursor != 0);
        if (set_data.size() != 0) {
            client->srem(_redis_pid_set_name, set_data.begin(), set_data.end());
        }

        std::map<std::string, std::string> hash_data;
        do {
            cursor = client->hscan(_redis_hash_name, cursor, pattern, std::inserter(hash_data, hash_data.begin()));
        } while (cursor != 0);
        if (hash_data.size() != 0) {
            std::vector<std::string> hash_keys;
            hash_keys.reserve(hash_data.size());

            std::transform(hash_data.begin(), hash_data.end(), std::back_inserter(hash_keys),
                [](const auto& pair) {
                return pair.first;
            });

            client->hdel(_redis_hash_name, hash_keys.begin(), hash_keys.end());
        }
    }

    void
    PgXidCollector::_internal_run()
    {
        // Create epoll instance to monitor file descriptors
        int epoll_fd = ::epoll_create1(0);
        PCHECK(epoll_fd !=  -1) << "epoll_create1(0) failed";

        // Buffer to hold incoming epoll events
        auto received_events = std::make_unique<struct epoll_event []>(static_cast<uint32_t>(LOOP_MAX_EVENTS));

        // Create a non-blocking, close-on-exec UNIX domain socket
        int unix_fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        PCHECK(unix_fd != -1) << "Failed to create UNIX domain socket";

        // Enable receiving credentials (SO_PASSCRED) from sending processes
        int enable = 1;
        CHECK(::setsockopt(unix_fd, SOL_SOCKET, SO_PASSCRED, &enable, sizeof(enable)) == 0)
            << "Failed to set SO_PASSCRED";

        struct sockaddr_un addr = {};
        addr.sun_family = AF_UNIX;

        // Use abstract namespace for socket name (starts with null byte)
        addr.sun_path[0] = '\0';
        ::memcpy(addr.sun_path + 1, _socket_name.data(), _socket_name.size());

        // Calculate full length of abstract address
        size_t addr_len = offsetof(struct sockaddr_un, sun_path) + 1 + _socket_name.size();
        PCHECK(::bind(unix_fd, (struct sockaddr*)&addr, addr_len) == 0) << "Failed to bind UNIX domain socket to address";

        // Prepare message reception structures
        constexpr size_t max_msg_count = 16;
        constexpr size_t msg_size = sizeof(PgXidCollectorMsg);
        constexpr size_t control_msg_len = CMSG_SPACE(sizeof(struct ucred));

        struct mmsghdr msgs_hdr[max_msg_count]{};
        struct iovec iovecs[msg_size]{};
        char _buffers[max_msg_count][msg_size]{};
        char _ctrls[max_msg_count][control_msg_len]{};

        // Initialize message headers and associated buffers
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

        // Register UNIX socket with epoll for edge-triggered read events
        struct epoll_event unix_event = {.events = (EPOLLET | EPOLLIN), .data = {}};
        PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, unix_fd, &unix_event) != -1) << "Failed to register unix domain socket fd";

        // Track active FDW processes by pid
        std::map<pid_t, std::shared_ptr<struct pid_data>> pid_event_storage;

        // Main event loop
        while (!_is_shutting_down()) {
            int event_count = ::epoll_wait(epoll_fd, received_events.get(), LOOP_MAX_EVENTS, LOOP_TIMEOUT_MSEC);
            if (event_count > 0) {
                for (int event_id = 0; event_id < event_count; ++event_id) {
                    uint64_t id = received_events[event_id].data.u64;
                    uint32_t events = received_events[event_id].events;
                    if (id == 0) {
                        // Handle UNIX socket event
                        CHECK(events & EPOLLIN) << "Received invalid event";

                        int rc = ::recvmmsg(unix_fd, msgs_hdr, max_msg_count, 0, nullptr);
                        PCHECK(rc > -1 || (errno == EWOULDBLOCK || errno == EAGAIN)) << "Unexpected error for recvmmsg() call";

                        for (int i = 0; i < rc; ++i) {
                            int len = msgs_hdr[i].msg_len;
                            DCHECK(len == msg_size) << "Unexpected message size";

                            // PgXidCollectorMsg contains db_id and xid
                            PgXidCollectorMsg *msg = reinterpret_cast<PgXidCollectorMsg *>(msgs_hdr[i].msg_hdr.msg_iov->iov_base);

                            struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msgs_hdr[i].msg_hdr);
                            DCHECK(cmsg != nullptr && cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_CREDENTIALS) << "Unexpected control message";
                            struct ucred *cred = (struct ucred *)CMSG_DATA(cmsg);

                            // If new FDW process, register its PID FD with epoll
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
                                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "FDW process {} added, new process count {}", cred->pid, pid_event_storage.size());
                            }

                            // Process update from FDW
                            _on_update(cred->pid, msg->db_id, msg->xid);
                        }
                    } else {
                        // Handle termination event from FDW process
                        pid_t pid = static_cast<pid_t>(id);
                        auto node = pid_event_storage.extract(pid);
                        CHECK(!node.empty());

                        auto process_data = std::move(node.mapped());
                        int process_fd = process_data->fd;
                        PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, process_fd, nullptr) != -1) << "Failed to remove process fd";
                        ::close(process_fd);
                        _on_fdw_death(pid);
                        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "FDW process {} died, remaining process count {}", pid, pid_event_storage.size());
                    }
                }
            } else {
                if (event_count == -1) {
                    PCHECK(errno == EINTR) << "epoll_wait() failed";
                    continue;
                }
            }
        }

        // Cleanup on shutdown

        // Remove and close UNIX domain socket FD
        PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, unix_fd, nullptr) != -1) << "Failed to remove unix domain socket fd";
        ::close(unix_fd);

        // Remove and close all PID FDs
        for (auto &item: pid_event_storage) {
            PCHECK(::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, item.second->fd, nullptr) != -1) << "Failed to remove process fd";
            ::close(item.second->fd);
        }

        // Close epoll FD
        ::close(epoll_fd);
    }

    void PgXidCollector::_cleanup_process(std::map<pid_t, std::pair<uint64_t, uint64_t>>::iterator it)
    {
        // Extract the db_id and xid previously associated with the given process
        const auto [old_db_id, old_xid] = it->second;

        // Locate the xid count map for the associated db_id
        auto db_it = _db_id_to_xid_to_count.find(old_db_id);
        CHECK(db_it != _db_id_to_xid_to_count.end());  // Sanity check: db_id entry must exist

        auto& xid_map = db_it->second;
        auto xid_it = xid_map.find(old_xid);
        // Sanity checks: xid entry must exist and xid count must be positive
        CHECK(xid_it != xid_map.end());
        CHECK(xid_it->second > 0);

        // Decrement the xid count for the process
        --(xid_it->second);
        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1,
            "Removing process data: process {}, db_id {}, xid {}, remaining xid count {}",
            it->first, old_db_id, old_xid, xid_it->second);

        // If xid count reaches zero, clean up the entry
        if (xid_it->second == 0) {
            xid_map.erase(xid_it);
        }
    }

    void PgXidCollector::_on_update(pid_t process_id, uint64_t db_id, uint64_t xid) noexcept
    {
        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Updating xid: process {}, db_id {}, xid {}", process_id, db_id, xid);
        bool notify_thread = false;
        // Protect shared structures during update
        std::unique_lock lock(_data_mutex);

        // Check if this process already has an xid record
        auto it = _pid_to_db_id_xid.find(process_id);
        if (it != _pid_to_db_id_xid.end()) {
            // Skip update if no change in db_id or xid
            if (it->second.first == db_id && it->second.second == xid) {
                return;
            }
            // Remove old xid association before updating
            _cleanup_process(it);
        } else {
            // New process observed — track and notify redis update thread
            _new_pids.push_back(std::make_pair(process_id, db_id));
            notify_thread = true;
        }

        // Associate process_id with new (db_id, xid) tuple
        _pid_to_db_id_xid.insert_or_assign(process_id, std::make_pair(db_id, xid));

        // Increment count of this xid under the current db_id
        auto& xid_map = _db_id_to_xid_to_count[db_id];
        ++xid_map[xid];
        lock.unlock();

        // Notify background thread to flush changes to Redis
        if (notify_thread) {
            _redis_cv.notify_all();
        }
    }

    void PgXidCollector::_on_fdw_death(pid_t pid) noexcept
    {
        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Process death: process {}", pid);
        // Safeguard against concurrent access
        std::unique_lock lock(_data_mutex);

        // Confirm that the process is tracked
        auto pid_it = _pid_to_db_id_xid.find(pid);
        CHECK(pid_it != _pid_to_db_id_xid.end());

        // Clean up all xid/db_id associations for the dead process
        _cleanup_process(pid_it);

        // Remove process from tracking
        _pid_to_db_id_xid.erase(pid_it);
    }

    void
    PgXidCollector::_internal_shutdown()
    {
        _redis_cv.notify_all();
        _redis_thread.join();
        _cleanup_redis_data();
    }

    void
    PgXidCollector::_redis_thread_run()
    {
        std::chrono::milliseconds dur(REDIS_UPDATE_INTERVAL_MSEC);

        while(!_is_shutting_down()) {
            std::vector<std::pair<std::string, std::string>> db_xid_list;
            std::vector<std::string> new_pid_list;

            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Redis data refresh; process count {}",  _pid_to_db_id_xid.size());

            // we will try to spend as little time as possible in the critical section
            std::shared_lock data_lock(_data_mutex);

            // collect min xids per database
            for (auto &[db_id, xid_map]: _db_id_to_xid_to_count) {
                uint64_t xid = constant::LATEST_XID;
                if (!xid_map.empty()) {
                    xid = xid_map.begin()->first;
                }
                std::string key = fmt::format("{}:{}", _fdw_id, db_id);
                std::string value = std::to_string(xid);
                db_xid_list.push_back(std::make_pair(key, value));
                LOG_DEBUG( LOG_FDW, LOG_LEVEL_DEBUG1, "Updating redis with key: {}, value: {}", key, value);
            }

            // move everything to a temporary set
            for (auto &[process_id, db_id]: _new_pids) {
                std::string value = fmt::format("{}:{}:{}", _fdw_id, db_id, process_id);
                new_pid_list.push_back(value);
            }
            _new_pids.clear();

            data_lock.unlock();

            // send data to redis
            RedisClientPtr client = RedisMgr::get_instance()->get_client();
            if (!db_xid_list.empty()) {
                client->hmset(_redis_hash_name, db_xid_list.begin(), db_xid_list.end());
            }
            if (!new_pid_list.empty()) {
                // NOTE: we are not checking here if correct number of process entries got deleted
                //      if the system got restarted, while FDW processes stayed up, then new process ids
                //      might not have an entry there as it was deleted by the previous run
                client->srem(_redis_pid_set_name, new_pid_list.begin(), new_pid_list.end());
            }

            data_lock.lock();
            _redis_cv.wait_for(data_lock, dur);
        }
    }
} // springtail::pg_fdw