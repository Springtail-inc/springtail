#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <queue>
#include <vector>

namespace springtail {
namespace common {

template <typename Container, typename Compare = std::less<typename Container::value_type>>
class SortedMerge {
public:
    using value_type = typename Container::value_type;
    using container_iterator = typename Container::const_iterator;
    using container_reverse_iterator = typename Container::const_reverse_iterator;
    using size_type = std::size_t;

private:
    template <typename IterType>
    struct IteratorEntryBase {
        IterType current;
        IterType end;
        size_type container_index;

        IteratorEntryBase(IterType curr, IterType e, size_type idx)
            : current(curr), end(e), container_index(idx) {}
    };

    using IteratorEntry = IteratorEntryBase<container_iterator>;
    using ReverseIteratorEntry = IteratorEntryBase<container_reverse_iterator>;

    template <typename Entry>
    struct ForwardHeapCompare {
        Compare comp;
        ForwardHeapCompare(const Compare& c) : comp(c) {}
        bool operator()(const Entry& a, const Entry& b) const {
            return comp(*b.current, *a.current);
        }
    };

    template <typename Entry>
    struct ReverseHeapCompare {
        Compare comp;
        ReverseHeapCompare(const Compare& c) : comp(c) {}
        bool operator()(const Entry& a, const Entry& b) const {
            return comp(*a.current, *b.current);
        }
    };

public:
    template <bool Reverse>
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = typename SortedMerge::value_type;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

    private:
        using entry_type = std::conditional_t<Reverse, ReverseIteratorEntry, IteratorEntry>;
        using heap_compare_type = std::conditional_t<Reverse,
            ReverseHeapCompare<entry_type>,
            ForwardHeapCompare<entry_type>>;
        using heap_type = std::priority_queue<entry_type, std::vector<entry_type>, heap_compare_type>;

        std::shared_ptr<heap_type> _heap;
        const value_type* current_value_;
        bool _is_end;

        void advance() {
            if (!_heap || _heap->empty()) {
                _is_end = true;
                current_value_ = nullptr;
                return;
            }

            entry_type top = _heap->top();
            _heap->pop();
            ++top.current;

            if (top.current != top.end) {
                _heap->push(top);
            }

            if (_heap->empty()) {
                _is_end = true;
                current_value_ = nullptr;
            } else {
                current_value_ = &(*_heap->top().current);
            }
        }

    public:
        Iterator() : _heap(nullptr), current_value_(nullptr), _is_end(true) {}

        explicit Iterator(const std::vector<Container>& containers, const Compare& comp)
            : _heap(std::make_shared<heap_type>(heap_compare_type(comp))),
              current_value_(nullptr),
              _is_end(false) {

            for (size_type i = 0; i < containers.size(); ++i) {
                if constexpr (Reverse) {
                    if (containers[i].rbegin() != containers[i].rend()) {
                        _heap->push(entry_type(containers[i].rbegin(), containers[i].rend(), i));
                    }
                } else {
                    if (containers[i].begin() != containers[i].end()) {
                        _heap->push(entry_type(containers[i].begin(), containers[i].end(), i));
                    }
                }
            }

            if (_heap->empty()) {
                _is_end = true;
            } else {
                current_value_ = &(*_heap->top().current);
            }
        }

        // Constructor for lower_bound/upper_bound with custom starting positions
        template <typename IterType>
        Iterator(const std::vector<Container>& containers,
                 const std::vector<std::pair<IterType, IterType>>& ranges,
                 const Compare& comp)
            : _heap(std::make_shared<heap_type>(heap_compare_type(comp))),
              current_value_(nullptr),
              _is_end(false) {

            for (size_type i = 0; i < ranges.size(); ++i) {
                if (ranges[i].first != ranges[i].second) {
                    if constexpr (Reverse) {
                        _heap->push(entry_type(ranges[i].first, ranges[i].second, i));
                    } else {
                        _heap->push(entry_type(ranges[i].first, ranges[i].second, i));
                    }
                }
            }

            if (_heap->empty()) {
                _is_end = true;
            } else {
                current_value_ = &(*_heap->top().current);
            }
        }

        reference operator*() const {
            return *current_value_;
        }

        pointer operator->() const {
            return current_value_;
        }

        Iterator& operator++() {
            advance();
            return *this;
        }

        Iterator operator++(int) {
            Iterator tmp = *this;
            advance();
            return tmp;
        }

        bool operator==(const Iterator& other) const {
            if (_is_end && other._is_end) {
                return true;
            }
            if (_is_end != other._is_end) {
                return false;
            }
            return current_value_ == other.current_value_;
        }

        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }
    };

    using iterator = Iterator<false>;
    using const_iterator = Iterator<false>;
    using reverse_iterator = Iterator<true>;
    using const_reverse_iterator = Iterator<true>;

    explicit SortedMerge(const std::vector<Container>& containers, const Compare& comp = Compare())
        : _containers(containers), _comp(comp) {}

    explicit SortedMerge(std::vector<Container>&& containers, const Compare& comp = Compare())
        : _containers(std::move(containers)), _comp(comp) {}

    iterator begin() const {
        return iterator(_containers, _comp);
    }

    iterator end() const {
        return iterator();
    }

    const_iterator cbegin() const {
        return const_iterator(_containers, _comp);
    }

    const_iterator cend() const {
        return const_iterator();
    }

    reverse_iterator rbegin() const {
        return reverse_iterator(_containers, _comp);
    }

    reverse_iterator rend() const {
        return reverse_iterator();
    }

    const_reverse_iterator crbegin() const {
        return const_reverse_iterator(_containers, _comp);
    }

    const_reverse_iterator crend() const {
        return const_reverse_iterator();
    }

    iterator lower_bound(const value_type& value) const {
        std::vector<std::pair<container_iterator, container_iterator>> ranges;
        ranges.reserve(_containers.size());

        for (const auto& container : _containers) {
            auto lb = std::lower_bound(container.begin(), container.end(), value, _comp);
            ranges.emplace_back(lb, container.end());
        }

        return iterator(_containers, ranges, _comp);
    }

    iterator upper_bound(const value_type& value) const {
        std::vector<std::pair<container_iterator, container_iterator>> ranges;
        ranges.reserve(_containers.size());

        for (const auto& container : _containers) {
            auto ub = std::upper_bound(container.begin(), container.end(), value, _comp);
            ranges.emplace_back(ub, container.end());
        }

        return iterator(_containers, ranges, _comp);
    }

    bool empty() const {
        for (const auto& container : _containers) {
            if (!container.empty()) {
                return false;
            }
        }
        return true;
    }

    size_type size() const {
        size_type total = 0;
        for (const auto& container : _containers) {
            total += container.size();
        }
        return total;
    }

private:
    std::vector<Container> _containers;
    Compare _comp;
};

template <typename Container, typename Compare = std::less<typename Container::value_type>>
SortedMerge<Container, Compare> make_sorted_merge(
    const std::vector<Container>& containers,
    const Compare& comp = Compare()) {
    return SortedMerge<Container, Compare>(containers, comp);
}

template <typename Container, typename Compare = std::less<typename Container::value_type>>
SortedMerge<Container, Compare> make_sorted_merge(
    std::vector<Container>&& containers,
    const Compare& comp = Compare()) {
    return SortedMerge<Container, Compare>(std::move(containers), comp);
}

}  // namespace common
}  // namespace springtail
