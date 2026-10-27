#include <common/constants.hh>
#include <storage/index_engine/gist_reader.hh>

/**
 * GistReader's consistent()-driven, fan-out-capable search -- the half of GistReader that needs
 * pg_ext (via GistReadStrategy::consistent() -> gist_helpers::consistent(), which dlopens and
 * calls into real Postgres opclass .so's). Deliberately its own translation unit, in its own
 * library (springtail_gist_engine, not springtail_storage) -- see gist_reader.cc's and
 * gist_helpers_opclass.cc's header comments for why: springtail_pg_fdw.so, which dlopens into a
 * real, unmodified Postgres backend, must never link pg_ext. GistReader's constructor (needed by
 * Table, which the FDW does link) lives in gist_reader.cc, which has no such dependency; nothing
 * in the FDW's own reachable code calls search() today (query-time GiST routing is phase 12's),
 * so this file is only ever linked into daemon-side consumers (springtail_sys_tbl_mgr,
 * pg_log_mgr_daemon, test_gist_strategy).
 */
namespace springtail {
    using gist_helpers::GistEntry;

    void GistReader::_search_recursive(uint64_t extent_id, const std::string &query_bytes,
                                       uint16_t strategy_num, std::vector<GistEntry> &out,
                                       size_t *leaves_visited) const {
        auto extent = _get_extent(extent_id);
        bool is_branch = extent->header().type.is_branch();

        if (!is_branch) {
            if (leaves_visited) {
                ++(*leaves_visited);
            }
            for (auto it = extent->begin(); it != extent->end(); ++it) {
                auto entry = _strategy.read_leaf(*it, _leaf_schema);
                if (_strategy.consistent(entry, 0, query_bytes, strategy_num, true)) {
                    out.push_back(entry);
                }
            }
            return;
        }

        for (auto it = extent->begin(); it != extent->end(); ++it) {
            auto entry = _strategy.read_branch(*it, _branch_schema);
            if (_strategy.consistent(entry, 0, query_bytes, strategy_num, false)) {
                _search_recursive(entry.child_id, query_bytes, strategy_num, out, leaves_visited);
            }
        }
    }

    std::vector<GistEntry> GistReader::search(const std::string &query_bytes, uint16_t strategy_num,
                                              size_t *leaves_visited) const {
        std::vector<GistEntry> out;
        if (leaves_visited) {
            *leaves_visited = 0;
        }
        if (_root_offset != constant::UNKNOWN_EXTENT) {
            _search_recursive(_root_offset, query_bytes, strategy_num, out, leaves_visited);
        }
        return out;
    }
}
