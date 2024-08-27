#pragma once
#include <ares.h>

#include <string>
#include <mutex>
#include <condition_variable>
#include <optional>

namespace springtail {

    /** DNS Resolver class */
    class DNSResolver {
    public:
        static constexpr int DNS_QCACHE_TTL_SECS = 15; ///< ttl for DNS cache
        static constexpr int DNS_TIMEOUT_SECS = 5;     ///< timeout for DNS query

        /** Get instance */
        static DNSResolver *get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        /** Shutdown */
        static void shutdown() {
            std::call_once(_shutdown_flag, _shutdown);
        }

        /** Resolve hostname to ip address string */
        std::optional<std::string> resolve(const std::string &hostname);

        /** Set TTL, shouldn't be done while service is in use */
        void set_ttl_secs(int ttl);

    private:
        /** Internal DNS request structure */
        struct DNSRequest {
            std::mutex mutex;
            std::condition_variable cv;
            std::string ip;
            int status;
            bool completed = false;
        };

        DNSResolver();

        // delete copy constructor
        DNSResolver(const DNSResolver &) = delete;
        void operator=(const DNSResolver &) = delete;

        static DNSResolver *_instance;        ///< instance
        static std::once_flag _init_flag;     ///< init flag
        static std::once_flag _shutdown_flag; ///< shutdown flag

        ares_channel_t *_channel = nullptr;   ///< c-ares channel

        /** init function */
        static void _init();

        /** static shutdown function */
        static void _shutdown();

        /** instance shutdown function */
        void shutdown_instance();

        /** c-ares callback */
        static void _addrinfo_cb(void *arg,
                            int status, int timeouts,
                            ares_addrinfo *addrinfo);
    };
}