#pragma once

#include <string>
#include <vector>

#include <common/logging.hh>
#include <common/singleton.hh>

namespace springtail {
class ServiceRunner {
public:
    explicit ServiceRunner(const std::string &name) : _name(name) {}
    virtual ~ServiceRunner() = default;
    virtual bool start()
    {
        LOG_INFO(LOG_ALL, "Default start for service {}", _name);
        return true;
    }
    virtual void stop() { LOG_INFO(LOG_ALL,"Default stop for service {}", _name); }
    const std::string &get_name() const { return _name; };

private:
    std::string _name;
};

class ServiceRegister : public Singleton<ServiceRegister> {
    friend class Singleton<ServiceRegister>;

public:
    bool start(std::vector<std::unique_ptr<ServiceRunner>> &service_list)
    {
        std::move(service_list.begin(), service_list.end(), std::back_inserter(_service_list));
        auto reverse_iter = _service_list.rend();
        for (auto iter = _service_list.begin(); iter != _service_list.end(); iter++) {
            LOG_INFO(LOG_ALL,"Starting service {}", (*iter)->get_name());
            if (!(*iter)->start()) {
                LOG_INFO(LOG_ALL,"Service {} failed to start", (*iter)->get_name());
                // set reverse iterator
                reverse_iter = std::make_reverse_iterator(iter);
                break;
            }
        }
        // check reverse iterator
        if (reverse_iter != _service_list.rend()) {
            while (reverse_iter != _service_list.rend()) {
                LOG_INFO(LOG_ALL,"Stopping service {}", (*reverse_iter)->get_name());
                (*reverse_iter)->stop();
                ++reverse_iter;
            }
            return false;
        }
        return true;
    }

private:
    ServiceRegister() = default;
    ~ServiceRegister() override = default;

    std::vector<std::unique_ptr<ServiceRunner>> _service_list;

    void _internal_shutdown() override
    {
        for (auto reverse_iter = _service_list.rbegin(); reverse_iter != _service_list.rend();
             reverse_iter++) {
            LOG_INFO(LOG_ALL,"Stoping service {}", (*reverse_iter)->get_name());
            (*reverse_iter)->stop();
        }
        _service_list.clear();
    }
};
};  // namespace springtail
