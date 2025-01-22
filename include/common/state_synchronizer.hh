#include <shared_mutex>
#include <atomic>
#include <condition_variable>
#include <string>
#include <set>

namespace springtail::common {

    /**
     * @brief Class to provide synchronization for state changes
     * @tparam StateEnum Enum type for state
     */
    template<typename StateEnum>
    class StateSynchronizer {
    public:
        explicit StateSynchronizer(StateEnum initial_state)
            : _current_state(initial_state) {}

        /** Set the state and notify waiting threads */
        void set(StateEnum new_state)
        {
            std::unique_lock<std::shared_mutex> lock(_mutex);
            _current_state = new_state;

            // Notify threads waiting for any state change
            lock.unlock();
            _cv.notify_all();
        }

        /** Wait for a specific state */
        void wait_for_state(std::set<StateEnum> desired_states)
        {
            std::unique_lock<std::shared_mutex> lock(_mutex);
            if (desired_states.contains(_current_state)) {
                return;
            }

            // wait for desired state
            _cv.wait(lock, [this, desired_states]() {
                return desired_states.contains(_current_state);
            });
        }

        /** Wait for a specific state */
        void wait_for_state(StateEnum desired_state)
        {
            wait_for_state(std::set<StateEnum>{desired_state});
        }

        /** Wait for desired state change and then atomically switch to new state */
        void wait_and_set(std::set<StateEnum> desired_states,
                          StateEnum new_state)
        {
            std::unique_lock<std::shared_mutex> lock(_mutex);
            if (desired_states.contains(_current_state)) {
                _current_state = new_state;
                lock.unlock();
                _cv.notify_all();
                return;
            }

            // wait for desired state
            _cv.wait(lock, [this, desired_states]() {
                return desired_states.contains(_current_state);
            });
            _current_state = new_state;
            lock.unlock();

            _cv.notify_all();
        }


        /** Wait for specific state and then atomically switch to new state */
        void wait_and_set(StateEnum desired_state,
                          StateEnum new_state)
        {
            wait_and_set(std::set<StateEnum>{desired_state}, new_state);
        }

        /** Test and set state */
        bool test_and_set(StateEnum check_state,
                          StateEnum new_state)
        {
            std::unique_lock<std::shared_mutex> lock(_mutex);
            if (_current_state == check_state) {
                _current_state = new_state;
                _cv.notify_all();
                return true;
            }
            return false;
        }

        /** Get the current state */
        StateEnum get() const
        {
            std::shared_lock<std::shared_mutex> lock(_mutex);
            return _current_state;
        }

        /** Check if the current state is equal to the given state */
        bool is(StateEnum state) const
        {
            std::shared_lock<std::shared_mutex> lock(_mutex);
            return _current_state == state;
        }

    private:
        StateEnum _current_state;         ///< Current state
        mutable std::shared_mutex _mutex; ///< Mutex for state
        std::condition_variable_any _cv;  ///< Condition variable for state
    };

}