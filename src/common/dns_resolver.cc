#include <stdio.h>

#include <ares.h>


#include <common/dns_resolver.hh>
#include <common/logging.hh>
#include <common/exception.hh>

namespace springtail {
    DNSResolver *DNSResolver::_instance = nullptr;
    std::once_flag DNSResolver::_init_flag;
    std::once_flag DNSResolver::_shutdown_flag;

    DNSResolver::DNSResolver()
    {
        // constructor -- see: https://c-ares.org/docs.html#example
        struct ares_options        options;
        int                        optmask = 0;

        /* Initialize library */
        ares_library_init(ARES_LIB_INIT_ALL);

        if (!ares_threadsafety()) {
            LOG_ERROR("c-ares is not thread safe");
            throw Error("c-ares is not thread safe");
        }

        // Enable event thread so we don't have to monitor file descriptors
        // options must have optmask set before they will be honored
        memset(&options, 0, sizeof(options));
        optmask      |= ARES_OPT_EVENT_THREAD;
        optmask      |= ARES_FLAG_NOSEARCH;
        optmask      |= ARES_FLAG_STAYOPEN;
        optmask      |= ARES_OPT_QUERY_CACHE;
        optmask      |= ARES_OPT_TIMEOUT;

        options.evsys = ARES_EVSYS_DEFAULT;
        options.qcache_max_ttl = DNS_QCACHE_TTL_SECS; // ttl for query cache
        options.timeout = DNS_TIMEOUT_SECS; // timeout for query

        // Initialize channel to run queries;
        // a single channel can accept unlimited queries
        if (ares_init_options(&_channel, &options, optmask) != ARES_SUCCESS) {
            LOG_ERROR("Failed to initialize c-ares channel");
            throw Error("Failed to initialize c-ares channel");
        }
    }

    void
    DNSResolver::set_ttl_secs(int ttl)
    {
        ares_options options;
        int optmask = 0;

        memset(&options, 0, sizeof(options));
        optmask      |= ARES_OPT_EVENT_THREAD;
        optmask      |= ARES_FLAG_NOSEARCH;
        optmask      |= ARES_FLAG_STAYOPEN;
        optmask      |= ARES_OPT_QUERY_CACHE;
        optmask      |= ARES_OPT_TIMEOUT;

        options.evsys = ARES_EVSYS_DEFAULT;
        options.qcache_max_ttl = ttl; // ttl for query cache
        options.timeout = DNS_TIMEOUT_SECS; // timeout for query

        if (ares_init_options(&_channel, &options, optmask) != ARES_SUCCESS) {
            LOG_ERROR("Failed to set DNS cache TTL");
            throw Error("Failed to set DNS cache TTL");
        }
    }

    void
    DNSResolver::_init()
    {
        _instance = new DNSResolver();
    }

    void
    DNSResolver::_shutdown()
    {
        if (_instance) {
            _instance->shutdown_instance();
            delete _instance;
            _instance = nullptr;
        }
    }

    void
    DNSResolver::shutdown_instance()
    {
        if (_channel == nullptr) {
            return;
        }

        /* Wait until no more requests are left to be processed */
        ares_queue_wait_empty(_channel, -1);

        /* Cleanup */
        ares_destroy(_channel);
        _channel = nullptr;

        ares_library_cleanup();
    }


    std::optional<std::string>
    DNSResolver::resolve(const std::string &hostname)
    {
        // resolve hostname
        struct ares_addrinfo_hints hints;

        /* Perform an IPv4 and IPv6 request for the provided domain name */
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET; // only IPv4 for now
        hints.ai_flags  = ARES_AI_CANONNAME;

        DNSRequest request; // init request object

        // issue request, passing in callback function and dns request state object
        ares_getaddrinfo(_channel, hostname.c_str(), nullptr, &hints,
                         _addrinfo_cb, &request);

        // lock mutex and wait on cv
        std::unique_lock<std::mutex> lock(request.mutex);
        request.cv.wait(lock, [&request] { return request.completed; });

        if (request.status != ARES_SUCCESS || request.ip.empty()) {
            LOG_ERROR("Failed to resolve hostname: {}", hostname);
            return std::nullopt;
        }

        return request.ip;
    }


    /* Callback that is called when DNS query is finished */
    void
    DNSResolver::_addrinfo_cb(void *arg,
                              int status, int timeouts,
                              ares_addrinfo *result)
    {
        DNSRequest *request = (DNSRequest *)arg;

        request->status = status;

        // check for error, if so cleanup and mark as completed
        if (status != ARES_SUCCESS || result == nullptr) {
            LOG_ERROR("Failed to resolve hostname: status: {}", status);
            if (result != nullptr) {
                ares_freeaddrinfo(result);
            }
            request->completed = true;
            request->cv.notify_one();
            return;
        }

        // go through the result and get the first IPv4 address
        ares_addrinfo_node *node;
        for (node = result->nodes; node != NULL; node = node->ai_next) {
            char        addr_buf[64] = "";
            const void *ptr          = NULL;

            // get the first IPv4 address
            if (node->ai_family == AF_INET) {
                const struct sockaddr_in *in_addr =
                (const struct sockaddr_in *)((void *)node->ai_addr);
                ptr = &in_addr->sin_addr;

                // return first AF_INET address
                ares_inet_ntop(node->ai_family, ptr, addr_buf, sizeof(addr_buf));
                request->ip = addr_buf;
                break;

            } else if (node->ai_family == AF_INET6) {
                // for completeness, but ignore IPv6 for now
                const struct sockaddr_in6 *in_addr =
                (const struct sockaddr_in6 *)((void *)node->ai_addr);
                ptr = &in_addr->sin6_addr;
            } else {
                continue;
            }
        }

        ares_freeaddrinfo(result);
        request->completed = true;
        request->cv.notify_one();
    }
}