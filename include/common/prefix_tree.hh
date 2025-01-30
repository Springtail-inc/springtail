#pragma once

#include <deque>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>

namespace springtail {
    template <typename T>
    class PrefixNode {
    public:
        PrefixNode() {}

        // the values won't be compared to previously added values
        // if the same value is added twice, then it will be there twice and
        // would have to be removed twice
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

        void collect_items(std::vector<std::pair<std::string, T>> &out_queue, const std::string path, std::deque<std::string> &node_path,
                std::function<bool (const T&)> select_fun) {
            // get the values from the current node
            for (const auto & value: _values) {
                if (select_fun(value)) {
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

        size_t count_items(std::deque<std::string> &node_path, std::function<bool (const T&)> select_fun) {
            size_t count = 0;
            for (const auto & value: _values) {
                if (select_fun(value)) {
                    count++;
                }
            }
            if (node_path.empty()) {
                // once the the in_queue is empty, walk the tree depth first
                for (const auto &child: _children) {
                    count += child.second->count_items(node_path, select_fun);
                }
            } else {
                // as long as the in_queue is not empty, follow the path
                std::string top_key = node_path.front();
                node_path.pop_front();
                if (_children.contains(top_key)) {
                    count += _children[top_key]->count_items(node_path, select_fun);
                }
            }
            return count;
        }

        size_t size() {
            std::deque<std::string> node_path = {};
            return count_items(node_path, [](const T&){ return true; });
        }
    private:
        std::map<std::string, std::shared_ptr<PrefixNode<T>>> _children;
        std::list<T> _values;
    };

};