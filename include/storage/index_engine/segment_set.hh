#pragma once

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <vector>

#include <absl/log/check.h>

namespace springtail {
    /**
     * Sealed, size-bounded segment files (architecture.md "btree" redesign Sec 7, phase-6):
     * a tree owns an ordered sequence of segment files instead of one ever-growing IOFile.
     * SegmentSet is the small, tree-shape-unaware mechanism that decides which segment is
     * "current" (the only one still being appended to) and when it seals and rolls over to a
     * fresh one -- IndexWriter owns one instance per tree, mirroring how it already owns
     * IndexExtentCache's process-wide pointer. Not itself the extent cache -- IndexExtentCache/
     * ExtentSource keep their existing (file, extent_id) key shape unmodified; SegmentSet's job
     * is purely to turn a packed real extent/root id into the (segment file path, in-segment
     * byte offset) pair that those existing, unmodified caches already know how to use as
     * `file`/`extent_id` -- see phases/phase-6.md's "Open questions resolved here" for why this
     * is "(file, segment_id, extent_id)" (plan.md's phrasing) realized as "(segment-specific
     * file path, in-segment offset)" fed into the existing (file, extent_id) shape, rather than
     * literally adding a third key field everywhere.
     *
     * Identity packing (open question 1, option A): a real, on-disk extent/root id is a single
     * uint64_t, packed as:
     *     bits [62:32] (31 bits) -- segment_id: monotonically increasing per tree, 0-based,
     *                               never reused once sealed.
     *     bits [31:0]  (32 bits) -- byte offset within that one segment (exactly what
     *                               IOFile::reserve_offset()/IOHandle::read() already use as a
     *                               plain per-file offset -- unchanged).
     * This leaves bit 63 untouched, so every real id this phase ever produces is < 2^63 ==
     * IndexWriter::TEMP_ID_BASE by construction (see the static_assert below), not by
     * convention -- a real id and a not-yet-flushed temp id remain structurally disjoint by
     * value alone, exactly as phase 1's TEMP_ID_BASE reservation already requires.
     *
     * 31 bits of segment_id (>2 billion segments) and 32 bits of in-segment offset (4GiB) are
     * both deliberately generous relative to the seal threshold this phase picks (tens of MiB,
     * see DEFAULT_SEAL_THRESHOLD_BYTES) -- a tree would have to accumulate an enormous number of
     * segments, or the seal threshold would have to be raised by two orders of magnitude, before
     * either range became a real constraint. Cheap to be generous here since both halves share
     * one fixed 63-bit budget.
     *
     * Segment file naming: segment 0 is the tree's own existing base file path, unchanged --
     * nothing before this phase (an already-on-disk single-segment tree) needs migrating, and
     * every raw-path debug tool (test/dump_headers.cc etc.) keeps working unmodified against a
     * tree that never grew past one segment. Segment N>=1 is a numbered sibling,
     * "<base-path>.<N>", in the same directory, following the brief's "numbered sibling of
     * today's single-file name" guidance. IOMgr::drop_all_fh()'s existing prefix match
     * (io_mgr.cc) already evicts every open handle for a tree's segments when handed the base
     * path as a prefix, with no change needed there.
     *
     * "Sealed" is never a separately-persisted bit -- it's always re-derived as "this segment's
     * current on-disk size has reached the seal threshold," checked fresh via stat() wherever it
     * matters (resume_from(), advance_after_flush()). This makes seal state trivially crash-
     * consistent: a process that dies after writing the byte that crosses the threshold but
     * before any seal-side-effect (registering with the Vacuumer) re-derives "already sealed"
     * identically on its next restart, from the file alone, no log/journal needed.
     *
     * Single-writer, no internal locking (architecture.md Sec 5.4) -- exactly one IndexWriter
     * owns one SegmentSet for the duration of one generation; nothing here coordinates
     * concurrent access, by the same contract IndexWriter itself relies on.
     */
    class SegmentSet {
    public:
        static constexpr int SEGMENT_ID_BITS = 31;
        static constexpr int OFFSET_BITS = 32;

        static_assert(SEGMENT_ID_BITS + OFFSET_BITS == 63,
                     "segment_id + offset must fill exactly the 63 bits below "
                     "IndexWriter::TEMP_ID_BASE (1ull << 63), so every packed real id is "
                     "structurally disjoint from the reserved temp-id range by construction");

        static constexpr uint64_t MAX_SEGMENT_ID = (uint64_t(1) << SEGMENT_ID_BITS) - 1;
        static constexpr uint64_t MAX_OFFSET = (uint64_t(1) << OFFSET_BITS) - 1;

        /** Default seal threshold (bytes of on-disk, compressed segment-file size) if not
            configured via Properties::get(Properties::STORAGE_CONFIG)'s
            "segment_seal_threshold_bytes" -- see phases/phase-6.md for why 64MiB was picked as a
            starting point, not a measured value (architecture.md Decisions log entry 6). */
        static constexpr uint64_t DEFAULT_SEAL_THRESHOLD_BYTES = 64ull * 1024 * 1024;

        /** Reads the configured default, falling back to DEFAULT_SEAL_THRESHOLD_BYTES. */
        static uint64_t default_seal_threshold_bytes();

        /**
         * pack()/segment_id_of()/offset_of()/segment_path() are deliberately trivial,
         * dependency-free, and `inline` (defined right here, not in segment_set.cc) -- both
         * IndexReader (read side, e.g. the pg_fdw-facing `where` binary) and IndexWriter (write
         * side) call these to translate between a real id and a (segment path, offset) pair, but
         * only IndexWriter's *stateful* seal/rotate bookkeeping (resume_from(),
         * advance_after_flush(), below, implemented in segment_set.cc) needs the Vacuumer.
         * Read-only consumers must never be forced to pull in segment_set.cc.o -- and,
         * transitively, vacuumer.cc.o and the full xid_mgr server library it needs -- just to
         * resolve these decode calls. Found by a real link failure (the `where` pg_fdw-test
         * executable, which links IndexReader's read-only dependency closure but not
         * springtail_xid_mgr) when these were first written as out-of-line calls into
         * segment_set.cc.
         *
         * pack()'s DCHECKs guard against a violation that would otherwise silently wrap into the
         * wrong segment or corrupt the top (TEMP_ID_BASE-reserved) bit.
         */
        static uint64_t pack(uint32_t segment_id, uint64_t offset) {
            DCHECK_LE(segment_id, MAX_SEGMENT_ID) << "segment_id space exhausted for this tree";
            DCHECK_LE(offset, MAX_OFFSET) << "segment grew past the in-segment offset range -- "
                                             "segment_seal_threshold_bytes is set too high";
            return (uint64_t(segment_id) << OFFSET_BITS) | offset;
        }

        static uint32_t segment_id_of(uint64_t real_id) {
            return static_cast<uint32_t>(real_id >> OFFSET_BITS);
        }

        static uint64_t offset_of(uint64_t real_id) {
            return real_id & MAX_OFFSET;
        }

        /** The on-disk path for `segment_id` of the tree rooted at `base_file`. */
        static std::filesystem::path segment_path(const std::filesystem::path &base_file,
                                                   uint32_t segment_id)
        {
            if (segment_id == 0) {
                return base_file;
            }
            return base_file.native() + "." + std::to_string(segment_id);
        }

        /** Inverse of segment_path(): the segment_id `path` corresponds to for `base_file`'s
            tree, or nullopt if `path` isn't one of this tree's segments at all (background
            space-reclaim compaction, architecture.md Sec 7.1, needs this to turn a Vacuumer-
            reported candidate path back into the segment_id its real ids pack against -- every
            other caller of this class already starts from a segment_id and goes forward). */
        static std::optional<uint32_t> segment_id_for_path(const std::filesystem::path &base_file,
                                                            const std::filesystem::path &path)
        {
            if (path == base_file) {
                return 0;
            }

            const std::string &base = base_file.native();
            const std::string &candidate = path.native();
            if (candidate.size() <= base.size() + 1 ||
                    candidate.compare(0, base.size(), base) != 0 ||
                    candidate[base.size()] != '.') {
                return std::nullopt;
            }

            const std::string suffix = candidate.substr(base.size() + 1);
            if (suffix.empty() || !std::ranges::all_of(suffix, [](char c) { return std::isdigit(c); })) {
                return std::nullopt;
            }

            errno = 0;
            char *end = nullptr;
            unsigned long parsed = std::strtoul(suffix.c_str(), &end, 10);
            if (errno != 0 || end == suffix.c_str() || *end != '\0' || parsed > MAX_SEGMENT_ID) {
                return std::nullopt;
            }
            return static_cast<uint32_t>(parsed);
        }

        explicit SegmentSet(std::filesystem::path base_file,
                           uint64_t seal_threshold_bytes = default_seal_threshold_bytes());

        /** The segment a fresh append should target right now. */
        std::filesystem::path current_path() const {
            return segment_path(_base_file, _current_segment_id);
        }

        uint32_t current_segment_id() const { return _current_segment_id; }

        /** Resume from an already-on-disk real id (IndexWriter::init()'s root_offset): decode
            its segment_id as the starting point, then immediately re-check seal state (see the
            class comment) in case that segment was already sealed by a previous generation --
            resuming at a stale, already-sealed segment_id would silently corrupt a file that
            must never be written to again. */
        void resume_from(uint64_t real_id);

        /** Must be called immediately after flushing a just-written extent to current_path();
            re-checks the current segment's on-disk size and seals (notifying the Vacuumer) plus
            rolls over to a fresh segment if the threshold has now been reached. */
        void advance_after_flush();

        /** Every segment path this tree has actually written at least one byte to so far
            (0..current, inclusive, but skipping a just-sealed-and-rotated-past current segment
            that nothing has been flushed into yet) -- used so sync() and the durability-fsync
            path (MutableTable::get_table_files()) cover every segment's bytes, not just the
            base file, without needlessly creating an empty file for a segment nothing has
            written to (IOSysFH's append-mode open is O_CREAT, so merely opening a
            not-yet-existing path to sync it would otherwise conjure an empty file into
            existence as a pure side effect). */
        std::vector<std::filesystem::path> all_paths() const;

    private:
        void _maybe_seal_and_rotate();

        std::filesystem::path _base_file;
        uint64_t _seal_threshold_bytes;
        uint32_t _current_segment_id = 0;
    };
}
