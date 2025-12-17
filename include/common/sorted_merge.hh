#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <queue>
#include <vector>

namespace springtail::common {

    /** SortedMerge is a utility class that merges multiple sorted containers into a single sorted sequence, K-way merge style.
     *
     * Template parameters:
     * - Container: must have value_type typedef
     * - ValueToKey: function object that defines key_type and extracts key from value
     *               Must have: typedef key_type; and key_type operator()(const value_type&) const;
     * - Compare: function object that compares keys (signature: bool(const key_type&, const key_type&))
     */
template <typename Container, typename ValueToKey, typename Compare>
class SortedMerge {
public:
    using value_type = typename Container::value_type;
    using key_type = typename ValueToKey::key_type;
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
        ValueToKey value_to_key;
        Compare comp;
        ForwardHeapCompare(const ValueToKey& vtk, const Compare& c) : value_to_key(vtk), comp(c) {}
        bool operator()(const Entry& a, const Entry& b) const {
            return comp(value_to_key(*b.current), value_to_key(*a.current));
        }
    };

    template <typename Entry>
    struct ReverseHeapCompare {
        ValueToKey value_to_key;
        Compare comp;
        ReverseHeapCompare(const ValueToKey& vtk, const Compare& c) : value_to_key(vtk), comp(c) {}
        bool operator()(const Entry& a, const Entry& b) const {
            return comp(value_to_key(*a.current), value_to_key(*b.current));
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
        using EntryType = std::conditional_t<Reverse, ReverseIteratorEntry, IteratorEntry>;
        using HeapCompareType = std::conditional_t<Reverse,
            ReverseHeapCompare<EntryType>,
            ForwardHeapCompare<EntryType>>;
        using HeapType = std::priority_queue<EntryType, std::vector<EntryType>, HeapCompareType>;

        std::shared_ptr<HeapType> _heap;
        const value_type* _current_value;
        bool _is_end;

        void advance() {
            if (!_heap || _heap->empty()) {
                _is_end = true;
                _current_value = nullptr;
                return;
            }

            EntryType top = _heap->top();
            _heap->pop();
            ++top.current;

            if (top.current != top.end) {
                _heap->push(top);
            }

            if (_heap->empty()) {
                _is_end = true;
                _current_value = nullptr;
            } else {
                _current_value = &(*_heap->top().current);
            }
        }

    public:
        Iterator() : _heap(nullptr), _current_value(nullptr), _is_end(true) {}

        explicit Iterator(std::vector<Container>& containers, const ValueToKey& value_to_key, const Compare& comp)
            : _heap(std::make_shared<HeapType>(HeapCompareType(value_to_key, comp))),
              _current_value(nullptr),
              _is_end(false) {

            for (size_type i = 0; i < containers.size(); ++i) {
                if constexpr (Reverse) {
                    if (containers[i].rbegin() != containers[i].rend()) {
                        _heap->push(EntryType(containers[i].rbegin(), containers[i].rend(), i));
                    }
                } else {
                    if (containers[i].begin() != containers[i].end()) {
                        _heap->push(EntryType(containers[i].begin(), containers[i].end(), i));
                    }
                }
            }

            if (_heap->empty()) {
                _is_end = true;
            } else {
                _current_value = &(*_heap->top().current);
            }
        }

        // Constructor for lower_bound/upper_bound with custom starting positions
        template <typename IterType>
        Iterator(const std::vector<std::pair<IterType, IterType>>& ranges,
                 const ValueToKey& value_to_key,
                 const Compare& comp)
            : _heap(std::make_shared<HeapType>(HeapCompareType(value_to_key, comp))),
              _current_value(nullptr),
              _is_end(false) {

            for (size_type i = 0; i < ranges.size(); ++i) {
                if (ranges[i].first != ranges[i].second) {
                    if constexpr (Reverse) {
                        _heap->push(EntryType(ranges[i].first, ranges[i].second, i));
                    } else {
                        _heap->push(EntryType(ranges[i].first, ranges[i].second, i));
                    }
                }
            }

            if (_heap->empty()) {
                _is_end = true;
            } else {
                _current_value = &(*_heap->top().current);
            }
        }

        reference operator*() const {
            return *_current_value;
        }

        pointer operator->() const {
            return _current_value;
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
            return _current_value == other._current_value;
        }
    };

    using iterator = Iterator<false>;
    using const_iterator = Iterator<false>;
    using reverse_iterator = Iterator<true>;
    using const_reverse_iterator = Iterator<true>;

    explicit SortedMerge(const std::vector<Container>& containers, const ValueToKey& value_to_key = ValueToKey(), const Compare& comp = Compare())
        : _containers(containers), _value_to_key(value_to_key), _comp(comp) {}

    explicit SortedMerge(std::vector<Container>&& containers, const ValueToKey& value_to_key = ValueToKey(), const Compare& comp = Compare())
        : _containers(std::move(containers)), _value_to_key(value_to_key), _comp(comp) {}

    iterator begin() const {
        return iterator(_containers, _value_to_key, _comp);
    }

    iterator end() const {
        return iterator();
    }

    const_iterator cbegin() const {
        return const_iterator(_containers, _value_to_key, _comp);
    }

    const_iterator cend() const {
        return const_iterator();
    }

    reverse_iterator rbegin() const {
        return reverse_iterator(_containers, _value_to_key, _comp);
    }

    reverse_iterator rend() const {
        return reverse_iterator();
    }

    const_reverse_iterator crbegin() const {
        return const_reverse_iterator(_containers, _value_to_key, _comp);
    }

    const_reverse_iterator crend() const {
        return const_reverse_iterator();
    }

    iterator lower_bound(const key_type& key) const {
        std::vector<std::pair<container_iterator, container_iterator>> ranges;
        ranges.reserve(_containers.size());

        for (auto& container : _containers) {
            auto lb = std::lower_bound(container.begin(), container.end(), key,
                [this](const value_type& val, const key_type& k) {
                    return _comp(_value_to_key(val), k);
                });
            ranges.emplace_back(lb, container.end());
        }

        return iterator(ranges, _value_to_key, _comp);
    }

    iterator upper_bound(const key_type& key) const {
        std::vector<std::pair<container_iterator, container_iterator>> ranges;
        ranges.reserve(_containers.size());

        for (auto& container : _containers) {
            auto ub = std::upper_bound(container.begin(), container.end(), key,
                [this](const key_type& k, const value_type& val) {
                    return _comp(k, _value_to_key(val));
                });
            ranges.emplace_back(ub, container.end());
        }

        return iterator(ranges, _value_to_key, _comp);
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

    void clear() {
        _containers.clear();
    }

private:
    mutable std::vector<Container> _containers;
    ValueToKey _value_to_key;
    Compare _comp;
};

template <typename Container, typename ValueToKey, typename Compare>
SortedMerge<Container, ValueToKey, Compare> make_sorted_merge(
    const std::vector<Container>& containers,
    const ValueToKey& value_to_key,
    const Compare& comp) {
    return SortedMerge<Container, ValueToKey, Compare>(containers, value_to_key, comp);
}

template <typename Container, typename ValueToKey, typename Compare>
SortedMerge<Container, ValueToKey, Compare> make_sorted_merge(
    std::vector<Container>&& containers,
    const ValueToKey& value_to_key,
    const Compare& comp) {
    return SortedMerge<Container, ValueToKey, Compare>(std::move(containers), value_to_key, comp);
}

}  // namespace springtail::common
