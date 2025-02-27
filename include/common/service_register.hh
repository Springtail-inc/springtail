#pragma once

#include <spdlog/spdlog.h>

#include <string>
#include <vector>

#include <common/singleton.hh>

namespace springtail {
class ServiceRunner {
public:
    explicit ServiceRunner(std::string name) : _name(name) {}
    virtual ~ServiceRunner() = default;
    virtual bool start()
    {
        SPDLOG_INFO("Default start for service {}", _name);
        return true;
    }
    virtual void stop() { SPDLOG_INFO("Default stop for service {}", _name); }
    const std::string &get_name() const { return _name; };

private:
    std::string _name;
};

class ServiceRegister : public Singleton<ServiceRegister> {
    friend class Singleton<ServiceRegister>;

public:
    bool start(std::vector<ServiceRunner *> &service_list)
    {
        _service_list = service_list;
        auto reverse_iter = _service_list.rend();
        for (auto iter = _service_list.begin(); iter != _service_list.end(); iter++) {
            SPDLOG_INFO("Starting service {}", (*iter)->get_name());
            if (!(*iter)->start()) {
                SPDLOG_INFO("Service {} failed to start", (*iter)->get_name());
                // set reverse iterator
                reverse_iter = std::make_reverse_iterator(iter);
                break;
            }
        }
        // check reverse iterator
        if (reverse_iter != _service_list.rend()) {
            while (reverse_iter != _service_list.rend()) {
                SPDLOG_INFO("Stopping service {}", (*reverse_iter)->get_name());
                (*reverse_iter)->stop();
            }
            return false;
        }
        return true;
    }

private:
    ServiceRegister() = default;
    ~ServiceRegister() override = default;

    std::vector<ServiceRunner *> _service_list;

    void _internal_shutdown() override
    {
        for (auto reverse_iter = _service_list.rbegin(); reverse_iter != _service_list.rend();
             reverse_iter++) {
            SPDLOG_INFO("Stoping service {}", (*reverse_iter)->get_name());
            (*reverse_iter)->stop();
            delete (*reverse_iter);
        }
        _service_list.clear();
    }
};
};  // namespace springtail
