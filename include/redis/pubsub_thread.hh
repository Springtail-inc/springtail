#pragma once

#include <thread>

#include <common/counter.hh>
#include <common/logging.hh>
#include <common/redis.hh>

namespace springtail {
    /**
     * @brief The intention of this class is to provide a conveniet way to receive notifications
     *      from redis. It handles channel subscriptions by calling provided callbacks on a
     *      background thread. Only a single subscription per channel is allowed.
     *      This class provides the following guarantees:
     *          1. All calls to redis::Subscriber must be done from one threads.
     *          2. All user callbacks must be called from internal thread(s).
     *          3. Functions start() and shutdown() will block until redis acknowledged
     *              all channel subscribe and unsubscribe actions respectively.
     */
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
            LOG_DEBUG(LOG_COMMON, "Added subscriber channel: {}", channel);
        }

        /**
         * @brief Shutdown function should be called from the main thread to trigger termination
         *          of pubsub thread.
         *
         */
        void shutdown() {
            LOG_DEBUG(LOG_COMMON, "Stopping subscriber thread {}", _id);
            _shutdown = true;
            _is_up.wait(true);
            _subscriber_thread.join();
            LOG_DEBUG(LOG_COMMON, "Joined subscriber thread {}", _id);
        }

        /**
         * @brief This function starts separate thread for receiving channel notifications from redis.
         *
         */
        void start() {
            _subscriber_thread = std::thread(&PubSubThread::_run, this);
            _is_up.wait(false);
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
        uint32_t _unconfirmed_channels{0};       ///< number of unconfirmed channels

        /**
         * @brief Process meta notification for given message type, channel, and number of registrations.
         *
         * @param type - message type
         * @param channel - channel name
         * @param num - number of registrations
         */
        void _process_meta(sw::redis::Subscriber::MsgType type, sw::redis::OptionalString channel, long long num)
        {
            LOG_DEBUG(LOG_COMMON, "received meta notification: message type: {}; channel: {}; num = {}",
                    static_cast<int>(type), channel, num);
            if (_unconfirmed_channels > 0) {
                LOG_DEBUG(LOG_COMMON, "received meta notification: processing");
                if ((type == sw::redis::Subscriber::MsgType::SUBSCRIBE ||
                     type == sw::redis::Subscriber::MsgType::UNSUBSCRIBE) &&
                            channel.has_value() && _channels.contains(channel.value())) {
                    _unconfirmed_channels--;
                }
            }
        }

        /**
         * @brief Setup function is called by start function. For each channel it calls init callback,
         *          subscribes to the channel, and waits for
         *          confirmation of subscription before moving to the next channel.
         *
         */
        virtual void _set_up() {
            _subscriber->on_meta([this](sw::redis::Subscriber::MsgType type, sw::redis::OptionalString channel, long long num){
                _process_meta(type, channel, num);
            });
            _unconfirmed_channels = _channels.size();
            for(const auto &_channel_pair: _channels) {
                auto &channel = _channel_pair.first;
                SubscriberInitCBFn init_fn = _channel_pair.second.first;
                _subscriber->subscribe(channel);
                _subscriber->on_message([this](const std::string &channel, const std::string &msg) {
                    LOG_DEBUG(LOG_COMMON, "Received notification on channel: {}, thread: {}", channel, _id);
                    auto it = this->_channels.find(channel);
                    if (it == this->_channels.end()) {
                        return;
                    }
                    SubscriberConsumeCBFn consume_fn = it->second.second;
                    consume_fn(msg);
                });
                init_fn();
            }
            while (_unconfirmed_channels > 0) {
                _subscriber->consume();
            }
            _is_up = true;
            _is_up.notify_one();
        };

        /**
         * @brief This function is called after the main loop is terminated. It usubscribes all registered the channels.
         *         This function is called by shutdown function.
         *
         */
        virtual void _tear_down() {
            _unconfirmed_channels = _channels.size();
            for(const auto &_channel_pair: _channels) {
                _subscriber->unsubscribe(_channel_pair.first);
            }
            while (_unconfirmed_channels > 0) {
                _subscriber->consume();
            }
            _is_up = false;
            _is_up.notify_one();
        };

        /**
         * @brief This is the function executed by the subscriber thread. It calls setup function, then executes the main
         *          loop till it receives shutdown signall, and at the end it calls teardown.
         *
         */
        void _run() {
            _set_up();
            _id = std::this_thread::get_id();
            LOG_DEBUG(LOG_COMMON, "Started subscriber thread {}", _id);
            while (!_shutdown) {
                try {
                    // consume from subscriber, timeout is set above
                    _subscriber->consume();
                } catch (const sw::redis::TimeoutError &e) {
                    // timeout, check for shutdown
                    continue;
                } catch (const sw::redis::Error &e) {
                    LOG_ERROR("Error consuming from redis: {} on thread {}\n", e.what(), _id);
                    break;
                }
            }
            _tear_down();
            LOG_DEBUG(LOG_COMMON, "Ended subscriber thread {}", _id);
        }
    };
};