#pragma once

#include <shared_mutex>
#include <thread>

#include <common/logging.hh>
#include <common/redis.hh>

namespace springtail {
    class PubSubThread {
    public:

        PubSubThread(int timeout, bool config_db) {
            _subscriber = RedisMgr::get_instance()->get_subscriber(timeout, config_db);
        }

        using SubscriberCBFn = std::function<void (const std::string &msg)>;
        void add_subscriber(std::string &channel, SubscriberCBFn cb_fn) {
            std::unique_lock lock(_subscriber_mutex);
            _channels.insert(std::pair<std::string, std::function<void(const std::string &)>>(channel, cb_fn));
            _subscriber->subscribe(channel);
            _subscriber->on_message([this](const std::string &channel, const std::string &msg) {
                std::shared_lock lock(_subscriber_mutex);
                auto it = this->_channels.find(channel);
                if (it == this->_channels.end()) {
                    return;
                }
                it->second(msg);
            });
        }

        void remove_subscriber(const std::string &channel) {
            std::unique_lock lock(_subscriber_mutex);
            if (_channels.erase(channel) != 0) {
                _subscriber->unsubscribe(channel);
            }
        }

        virtual void setup() {};
        virtual void teardown() {};

        void run() {
            SPDLOG_DEBUG("Started subscriber thread {}", _subscriber_thread.get_id());
            while (!_shutdown) {
                try {
                    // consume from subscriber, timeout is set above
                    _subscriber->consume();
                } catch (const sw::redis::TimeoutError &e) {
                    // timeout, check for shutdown
                    continue;
                } catch (const sw::redis::Error &e) {
                    SPDLOG_ERROR("Error consuming from redis: {}\n", e.what());
                    break;
                }
            }
        }

        void shutdown() {
            std::thread::id id = _subscriber_thread.get_id();
            SPDLOG_DEBUG("Stopping subscriber thread {}", id);
            _shutdown = true;
            _subscriber_thread.join();
            std::unique_lock lock(_subscriber_mutex);
            for(const auto &_channel_pair: _channels) {
                _subscriber->unsubscribe(_channel_pair.first);
            }
            _channels.clear();
            lock.unlock();
            teardown();
            SPDLOG_DEBUG("Joined subscriber thread {}", id);
        }

        void start() {
            setup();
            _subscriber_thread = std::thread(&PubSubThread::run, this);

        }
    protected:
        std::atomic<bool> _shutdown = false;
        std::map<std::string, std::function<void(const std::string &)>> _channels;
        RedisMgr::SubscriberPtr _subscriber;
        std::thread _subscriber_thread;
        std::shared_mutex _subscriber_mutex;
    };
};