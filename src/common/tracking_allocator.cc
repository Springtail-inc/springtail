#include <iostream>
#include <set>

#include <common/tracking_allocator.hh>

namespace springtail {
    std::atomic<std::size_t> TrackingAllocatorStats::_allocated_bytes = 0;
    TrackingAllocatorStats *TrackingAllocatorStats::_instance {nullptr};

    std::once_flag TrackingAllocatorStats::_init_flag;

    void
    TrackingAllocatorStats::init() {
        _instance = new TrackingAllocatorStats();
    }
};
