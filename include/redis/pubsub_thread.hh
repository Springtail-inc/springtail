#pragma once

#include <shared_mutex>
#include <thread>
#include <ostream>

#include <common/logging.hh>
#include <common/redis.hh>

namespace springtail {
    class PubSubThread {
    public:

        /**
         * @brief Construct a new Pub Sub Thread object
         *
         * @param timeout - read timeout on connection to redis
         * @param config_db - indicates which redis database to use:
         *                  true - use config db
         *                  false - use data db
         */
        PubSubThread(int timeout, bool config_db) {
            _subscriber = RedisMgr::get_instance()->get_subscriber(timeout, config_db);
        }

        /**
         * @brief typedef for initialization callback
         *
         */
        using SubscriberInitCBFn = std::function<void ()>;
        /**
         * @brief typedef for consume callback
         *
         */
        using SubscriberConsumeCBFn = std::function<void (const std::string &msg)>;

        /**
         * @brief Function for registering channel subscribers. Right now we can have only one
         *          subscriber per channel. For now this function can only be called before start
         *          function.
         *
         * @param channel - channel name
         * @param init_fn - init callback
         * @param consume_fn - consume callback
         */
        void add_subscriber(std::string &channel, SubscriberInitCBFn init_fn, SubscriberConsumeCBFn consume_fn) {
            assert(!_is_up);
            _channels.insert(std::pair(channel, std::pair(init_fn, consume_fn)));
            SPDLOG_DEBUG("Added subscriber channel: {}", channel);
        }

        /**
         * @brief Shutdown function should be called from the main thread to trigger termination
         *          of pubsub thread.
         *
         */
        void shutdown() {
            SPDLOG_DEBUG("Stopping subscriber thread {}", _id);
            _shutdown = true;
            _subscriber_thread.join();
            SPDLOG_DEBUG("Joined subscriber thread {}", _id);
        }

        /**
         * @brief This function starts separate thread for receiving channel notifications from redis.
         *
         */
        void start() {
            _subscriber_thread = std::thread(&PubSubThread::_run, this);
        }

        /**
         * @brief Function to check if pubsub thread is up and ready to receive and process redis notifications.
         *
         * @return true
         * @return false
         */
        bool is_up() {
            return _is_up;
        }
    protected:
        std::atomic<bool> _shutdown = false;    ///< shutdown atomic
        std::atomic<bool> _is_up = false;       ///< "is up" atomic
        std::map<std::string, std::pair<SubscriberInitCBFn, SubscriberConsumeCBFn>> _channels;  ///< collection of channels with the associated init and consume callbacks
        RedisMgr::SubscriberPtr _subscriber;    ///< redis subscriber object
        std::thread _subscriber_thread;         ///< subscriber thread
        std::thread::id _id;                    ///< subscriber thread id

        /**
         * @brief Setup function is run inside the subscriber thread right after it starts and before it executes
         *          the main loop. It subscribes to all registered channels and calls init callback for each channel.
         *          This specific order is required to ensure that we do not miss any notifications after the data
         *          initialization.
         *
         */
        virtual void _set_up() {
            for(const auto &_channel_pair: _channels) {
                auto &channel = _channel_pair.first;
                SubscriberInitCBFn init_fn = _channel_pair.second.first;
                _subscriber->subscribe(channel);
                _subscriber->on_message([this](const std::string &channel, const std::string &msg) {
                    SPDLOG_DEBUG("Received notification on channel: {}, thread: {}", channel, _id);
                    auto it = this->_channels.find(channel);
                    if (it == this->_channels.end()) {
                        return;
                    }
                    SubscriberConsumeCBFn consume_fn = it->second.second;
                    consume_fn(msg);
                });
                init_fn();
            }
            _is_up = true;
        };

        /**
         * @brief This function is called after the main loop is terminated. It usubscribes all registered the channels
         *          and performs cleanup.
         *
         */
        virtual void _tear_down() {
            _is_up = false;
            for(const auto &_channel_pair: _channels) {
                _subscriber->unsubscribe(_channel_pair.first);
            }
            _channels.clear();
        };

        /**
         * @brief This is the function executed by the subscriber thread. It calls setup function, then executes the main
         *          loop till it receives shutdown signall, and at the end it calls teardown.
         *
         */
        void _run() {
            _id = std::this_thread::get_id();
            SPDLOG_DEBUG("Started subscriber thread {}", _id);
            _set_up();
            while (!_shutdown) {
                try {
                    // consume from subscriber, timeout is set above
                    _subscriber->consume();
                } catch (const sw::redis::TimeoutError &e) {
                    // timeout, check for shutdown
                    continue;
                } catch (const sw::redis::Error &e) {
                    SPDLOG_ERROR("Error consuming from redis: {} on thread {}\n", e.what(), _id);
                    break;
                }
            }
            _tear_down();
            SPDLOG_DEBUG("Ended subscriber thread {}", _id);
        }
    };
};