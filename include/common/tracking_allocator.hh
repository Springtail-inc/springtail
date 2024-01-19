#pragma once

#include <memory>
#include <atomic>
#include <mutex>

#include <iostream>

namespace springtail {

    /**
     * @brief Set of tags used for the TrackingAllocator.
     * For each tag, a forward declaration should be added in tracking_allocator.cc
     * E.g.: template class TrackingAllocatorStats<TrackingAllocatorTags::TAG_WRITE_CACHE>;
     */
    struct TrackingAllocatorTags {
        static constexpr char TAG_WRITE_CACHE[] = "write_cache";
    };

    /**
     * @brief Singleton: Maintain allocation stats (number of bytes) for custom allocator.
     * Used by TrackingAllocator below.
     * Usage: TrackingAllocatorStats<tag_string>::get_instance()->get_allocated_bytes();
     * @tparam *TAG TAG string defined in TrackingAllocatorTags struct above.
     */
    template<const char *TAG>
    class TrackingAllocatorStats
    {
    public:
        TrackingAllocatorStats(const TrackingAllocatorStats<TAG>&) = delete;
        TrackingAllocatorStats& operator=(const TrackingAllocatorStats<TAG> &) = delete;
        TrackingAllocatorStats& operator=(const TrackingAllocatorStats<TAG> &&) = delete;

        /** allocate; incr bytes allocated */
        void allocate(std::size_t size) { _allocated_bytes += size; }
        /** deallocate; decr bytes allocated */
        void deallocate(std::size_t size) { _allocated_bytes -= size; }
        /** get number of allocated bytes */
        std::size_t get_allocated_bytes() { return _allocated_bytes.load(); }

        /** get singleton instance */
        static TrackingAllocatorStats<TAG> *get_instance() {
            std::call_once(_init_flag, &TrackingAllocatorStats<TAG>::init);
            return _instance;
        }

    private:
        TrackingAllocatorStats() {};

        /** Initializer for singleton, called once */
        static void init();

        /** singleton instance */
        static TrackingAllocatorStats<TAG> *_instance;

        /** once flag for once initialization */
        static std::once_flag _init_flag;

        /** atomic byte counter */
        static std::atomic<std::size_t> _allocated_bytes;
    };

    template<const char *TAG>
    std::once_flag TrackingAllocatorStats<TAG>::_init_flag;

    template<const char *TAG>
    TrackingAllocatorStats<TAG> *TrackingAllocatorStats<TAG>::_instance {nullptr};

    template<const char *TAG>
    std::atomic<std::size_t> TrackingAllocatorStats<TAG>::_allocated_bytes = 0;

    /**
     * @brief Tracking allocator, inherit from std allocator, uses singleton tracking stats
     * to keep track of bytes allocated.
     * @tparam T    Type for allocation e.g., int, std::string, etc.
     * @tparam *TAG TAG string defined in TrackingAllocatorTags struct above.
     */
    template<typename T, const char *TAG>
    class TrackingAllocator : public std::allocator<T> {
    public:
        typedef typename std::allocator<T>::pointer pointer;
        typedef typename std::allocator<T>::size_type size_type;

        TrackingAllocator() noexcept = default;

        /** Inherit constructors for compatibility */
        template <typename U>
        TrackingAllocator(const TrackingAllocator<U, TAG>& other) noexcept : std::allocator<T>(other) {}

        /** allocate memory returning pointer; update tracker */
        pointer allocate(size_type n) {
            auto* p = std::allocator<T>::allocate(n);
            TrackingAllocatorStats<TAG>::get_instance()->allocate(n * sizeof(T));
            return p;
        }

        /** deallocate memory based on pointer; update tracker */
        void deallocate(pointer p, size_type n) noexcept {
            std::allocator<T>::deallocate(p, n);
            TrackingAllocatorStats<TAG>::get_instance()->deallocate(n * sizeof(T));
        }

        /** Rebind allocator for different types */
        template <typename U>
        struct rebind {
            typedef TrackingAllocator<U, TAG> other;
        };
    };


/*
    // Equality comparison for compatibility
    template <typename S, typename U>
    bool operator==(const TrackingAllocator<S>&, const TrackingAllocator<U>&) {
        return true;
    }

    template <typename S, typename U>
    bool operator!=(const TrackingAllocator<S>&, const TrackingAllocator<U>&) {
        return false;
    }
    */
};