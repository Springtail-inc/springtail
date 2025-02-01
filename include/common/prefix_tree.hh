#pragma once

#include <deque>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>

namespace springtail {
    /**
     * @brief PrefixNode class that implements prefix tree
     *
     * @tparam T - value type for the list of values stored at each node
     */
    template <typename T>
    class PrefixNode {
    public:
        /**
         * @brief Construct a new Prefix Node object
         *
         */
        PrefixNode() = default;

        /**
         * @brief Add value item to the specified path
         *          the code of this function recursively walks the path till
         *          it reaches the level at which the data should be stored.
         *      NOTE: the values stored at the node level won't be compared to the ones that
         *          are already stored there. If the same value is added more than once,
         *          then it will be there as many times as it has been added and
         *          also would have to be removed as many times as it has been added
         *
         * @param node_path - queue of strings representing the path down from this node
         * @param value - value to store
         */
        void add_item(std::deque<std::string> &node_path, const T &value) {
            if (node_path.empty()) {
                _values.push_back(value);
            } else {
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (!_children.contains(top_key)) {
                    _children[top_key] = std::make_shared<PrefixNode<T>>();
                }
                _children[top_key]->add_item(node_path, value);
            }
        }

        // if the path is wrong and points to non-existing branches, nothing will happen
        /**
         * @brief Remove value item from the specified path
         *          the code of this function recursively walks the path till
         *          it reaches the level at which the data should be stored.
         *          If the path is wrong and points to non-existing branches, nothing will
         *          will be removed.
         *
         * @param node_path
         * @param value
         */
        void remove_item(std::deque<std::string> &node_path, const T &value) {
            if (node_path.empty()) {
                auto iter = std::find(_values.begin(), _values.end(), value);
                if (iter != _values.end()) {
                    _values.erase(iter);
                }
            } else {
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (_children.contains(top_key)) {
                    _children[top_key]->remove_item(node_path, value);
                }
            }
        }

        /**
         * @brief Collect the stored items from the tree in a vector (queue) of pairs that contain
         *          a path where the value item was found and the value item itself. The item selection
         *          is based on criteria implemented in select function.
         *
         * @param out_queue - the queue of path and value item pairs
         * @param path - prefix path to use on each level
         * @param node_path - the path that specifies where the value items should come from
         *                      as long as the path is not empty, the value items will be;
         *                      collected at the nodes visited by the path; if the path is empty,
         *                      then all the nodes under this path will be visited
         * @param select_fun - function for selection criteria
         */
        void collect_items(std::vector<std::pair<std::string, T>> &out_queue, const std::string &path, std::deque<std::string> &node_path,
                std::function<bool (const std::string &, const T&)> select_fun) {
            // get the values from the current node
            for (const auto & value: _values) {
                if (select_fun(path, value)) {
                    out_queue.push_back(std::make_pair(path, value));
                }
            }
            if (node_path.empty()) {
                // once the in_queue is empty, walk the tree depth first
                for (const auto &child: _children) {
                    std::string next_path = path + "/" + child.first;
                    child.second->collect_items(out_queue, next_path, node_path, select_fun);
                }
            } else {
                // as long as the in_queue is not empty, follow the path
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (_children.contains(top_key)) {
                    std::string next_path = path + "/" + top_key;
                    _children[top_key]->collect_items(out_queue, next_path, node_path, select_fun);
                }
            }
        }

        /**
         * @brief Count the number of items stored in the tree that satisfy selection criteria
         *
         * @param node_path - path to follow through the tree
         * @param path - prefix path to use on each level
         * @param select_fun - function for selection criteria
         * @return size_t - number of items on the path and under it that satisfy selectio criteria
         */
        size_t count_items(std::deque<std::string> &node_path, const std::string &path, std::function<bool (const std::string &, const T&)> select_fun) {
            size_t count = 0;
            for (const auto & value: _values) {
                if (select_fun(path, value)) {
                    count++;
                }
            }
            if (node_path.empty()) {
                // once the the in_queue is empty, walk the tree depth first
                for (const auto &child: _children) {
                    std::string next_path = path + "/" + child.first;
                    count += child.second->count_items(node_path, next_path, select_fun);
                }
            } else {
                // as long as the in_queue is not empty, follow the path
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (_children.contains(top_key)) {
                    std::string next_path = path + "/" + top_key;
                    count += _children[top_key]->count_items(node_path, next_path, select_fun);
                }
            }
            return count;
        }

        /**
         * @brief The total number of value items stored in the tree
         *
         * @return size_t - number of items
         */
        size_t size() {
            std::deque<std::string> node_path = {};
            return count_items(node_path, [](const std::string &, const T&){ return true; });
        }
    private:
        /**
         * @brief Map of branches from the current node. It maps a name to on of the child nodes
         *
         */
        std::map<std::string, std::shared_ptr<PrefixNode<T>>> _children;
        /**
         * @brief A list of values of type T associated with the current node.
         *
         */
        std::list<T> _values;
    };

};