#include <common/tracking_allocator.hh>
#include <iostream>
#include <set>

namespace springtail {
// Define static init function for Tracking allocator stats
template <const char *TAG>
void
TrackingAllocatorStats<TAG>::init()
{
    _instance = new TrackingAllocatorStats();
}

// forward declaration of TrackingAllocatorStats using the write cache tag
template class TrackingAllocatorStats<TrackingAllocatorTags::TAG_WRITE_CACHE>;
};  // namespace springtail
