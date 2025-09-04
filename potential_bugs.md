# Potential Bugs and Proposed Fixes

This document captures key issues found in the storage layer and proposes targeted patches. Each item links to a patch in `patches/` that implements the suggested fix.

1) BTree branch traversal can prematurely return end()
- Problem: In `src/storage/btree.cc`, `lower_bound`/`upper_bound` return `end()` if the branch-level search returns `end()`, skipping the proper last child.
- Impact: Misses valid rows near the high-key range.
- Patch: `patches/01-btree-branch-fallback-lower-upper-bound.patch` — always fall back to the last child at branch level.

2) Missing read locks in Page accessors/search
- Problem: `StorageCache::Page` accessors (`begin/last/lower_bound/upper_bound/at`) read `_extents` without taking a shared lock.
- Impact: Data races against writers; iterator invalidation during concurrent modification.
- Patch: `patches/02-page-read-locks-for-iterators-and-search.patch` — add `boost::shared_lock` to relevant accessors.

3) Mutex/condition-variable misuse with adopt_lock
- Problem: Several places use `boost::unique_lock(..., adopt_lock)` without owning the mutex, and call `lock.release()`. Also notify/wait ordering is fragile.
- Impact: Undefined behavior, sporadic deadlocks or missed wakeups.
- Patch: `patches/03-cache-locking-cv-fixes-adoptlock-removal.patch` — replace adopt_lock with explicit unique_lock where needed, fix `_read_extent` CV protocol, and correct waiter logic.

4) IO short-read handling for vector reads
- Problem: `IOSysFH::read` assumes `preadv` reads the full vector; partial reads can occur.
- Impact: Assertion failures or decode errors on short reads.
- Patch: `patches/04-io-read-handle-partial-reads.patch` — handle short reads by topping up with `pread` and scatter-copying remainder.

5) Extent iterator underflow on decrement
- Problem: `Extent::Iterator::operator--` decrements unconditionally; underflow if used at `begin()`.
- Impact: Undefined behavior if misused.
- Patch: `patches/05-extent-iterator-decrement-guard.patch` — add an assertion guard before decrement.

Notes / Future Work (not patched yet)
- Variable-area compaction: `Extent::remove` leaves variable blobs; consider compaction or refcounts.
- Broader locking cleanup: additional adopt_lock sites exist; consider broader refactor once validated.
