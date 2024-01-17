#pragma once

#include <memory>
#include <atomic>
#include <mutex>

#include <iostream>

namespace springtail {

    /**
     * Singleton: Maintain allocation stats (number of bytes) for custom allocator
     */
    class TrackingAllocatorStats
    {
    public:
        TrackingAllocatorStats(const TrackingAllocatorStats&) = delete;
        TrackingAllocatorStats& operator=(const TrackingAllocatorStats &) = delete;
        TrackingAllocatorStats& operator=(const TrackingAllocatorStats &&) = delete;

        /** allocate; incr bytes allocated */
        void allocate(std::size_t size) { _allocated_bytes += size; }
        /** deallocate; decr bytes allocated */
        void deallocate(std::size_t size) { _allocated_bytes -= size; }
        /** get number of allocated bytes */
        std::size_t get_allocated_bytes() { return _allocated_bytes.load(); }

        /** get singleton instance */
        static TrackingAllocatorStats *get_instance() {
            std::call_once(_init_flag, &TrackingAllocatorStats::init);
            return _instance;
        }

    private:
        TrackingAllocatorStats() {};

        /** Initializer for singleton, called once */
        static void init();

        /** singleton instance */
        static TrackingAllocatorStats *_instance;

        /** once flag for once initialization */
        static std::once_flag _init_flag;

        /** atomic byte counter */
        static std::atomic<std::size_t> _allocated_bytes;
    };

    /**
     * Tracking allocator, inherit from std allocator, uses singleton tracking stats
     * to keep track of bytes allocated
     */
    template<typename T>
    class TrackingAllocator : public std::allocator<T> {
    public:
        typedef typename std::allocator<T>::pointer pointer;
        typedef typename std::allocator<T>::size_type size_type;

        TrackingAllocator() noexcept = default;

        /** Inherit constructors for compatibility */
        template <typename U>
        TrackingAllocator(const TrackingAllocator<U>& other) noexcept : std::allocator<T>(other) {}

        /** allocate memory returning pointer; update tracker */
        pointer allocate(size_type n) {
            auto* p = std::allocator<T>::allocate(n);
            TrackingAllocatorStats::get_instance()->allocate(n * sizeof(T));
            std::cout << "Allocating: " << typeid(T).name() << ", number: " << n << ", size: " << sizeof(T) << std::endl;
            return p;
        }

        /** deallocate memory based on pointer; update tracker */
        void deallocate(pointer p, size_type n) noexcept {
            std::allocator<T>::deallocate(p, n);
            TrackingAllocatorStats::get_instance()->deallocate(n * sizeof(T));
            std::cout << "Deallocating: " << (n * sizeof(T)) << std::endl;
        }

        /** Rebind allocator for different types */
        template <typename U>
        struct rebind {
            typedef TrackingAllocator<U> other;
        };
    };



    // Equality comparison for compatibility
    template <typename S, typename U>
    bool operator==(const TrackingAllocator<S>&, const TrackingAllocator<U>&) {
        return true;
    }

    template <typename S, typename U>
    bool operator!=(const TrackingAllocator<S>&, const TrackingAllocator<U>&) {
        return false;
    }
};