#include <common/concurrent_queue.hh>
#include <pg_log_mgr/xid_ready.hh>

namespace springtail::pg_log_mgr {
    ConcurrentQueue<XidReady> _committer_queue; // Queue for table operations
}