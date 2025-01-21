#pragma once

#include <set>

namespace springtail::pg_proxy {
    /** Something that is runnable by the proxy server */
    class Runnable {
    public:
        Runnable() = default;
        virtual ~Runnable() = default;

        /** entry point for thread pool to run */
        void operator()() {
            run(fds);
        }

        /** entry point for implementor to run */
        virtual void run(const std::set<int> &fds) = 0;

        /** file descriptors that triggered the event */
        std::set<int> fds;
    };
    using RunnablePtr = std::shared_ptr<Runnable>;
}