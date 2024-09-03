#pragma once

#include <memory>
#include <mutex>

namespace springtail {

    /** Metrics singleton */
    class Metrics {
    public:
        Metrics *get_instance() {
            std::call_once(_init_flag, &Metrics::_init);
            return _instance;
        }

    private:
        Metrics();
        Metrics(const Metrics&) = delete;
        Metrics(const Metrics&&) = delete;
        Metrics& operator=(const Metrics&) = delete;
        Metrics& operator=(const Metrics&&) = delete;

        static Metrics *_instance;
        static std::once_flag _init_flag;
        static std::once_flag _shutdown_flag;

        static void _init() {
            _instance = new Metrics();
        }
    };
}