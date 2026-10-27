#include <common/constants.hh>
#include <storage/index_engine/gist_reader.hh>

/**
 * This file holds only the opclass-independent half of GistReader: construction and the plain
 * (non-predicate) full-tree walk (all_leaves()/_collect_all(), which only need read_leaf/
 * read_branch -- pure row decode, no opclass calls). GistReader::search()/_search_recursive(),
 * which call consistent() (needing pg_ext), live in the sibling gist_reader_search.cc instead,
 * deliberately -- see that file's header comment and src/storage/CMakeLists.txt's
 * springtail_gist_engine comment for why: `Table` (sys_tbl_mgr/table.cc) constructs a GistReader
 * from both the daemon-side and the FDW-side build of sys_tbl_mgr (springtail_sys_tbl_mgr and
 * springtail_sys_tbl_mgr_client respectively), and the FDW-side one ends up inside
 * springtail_pg_fdw.so, which must never pull in pg_ext (see gist_helpers_opclass.cc's header
 * comment for the confirmed backend-crash this caused once). search() isn't called from
 * anywhere reachable by the FDW today (query-time GiST routing is phase 12's), but splitting it
 * into its own translation unit keeps that true at the *linker* level, not just by convention.
 */
namespace springtail {
    using gist_helpers::GistEntry;

    GistReader::GistReader(uint64_t database_id, const std::filesystem::path &file, uint64_t xid,
                           uint64_t root_offset, ExtentSourcePtr source, GistReadStrategy strategy)
        : _database_id(database_id),
          _file(file),
          _strategy(std::move(strategy)),
          _source(std::move(source)),
          _xid(xid),
          _root_offset(root_offset)
    {
        _leaf_schema = gist_helpers::build_leaf_schema(_strategy.opclass_names());
        _branch_schema = gist_helpers::build_branch_schema(_strategy.opclass_names());
        _branch_child_f = _branch_schema->get_field(constant::BTREE_CHILD_FIELD);
    }

    std::shared_ptr<Extent> GistReader::_get_extent(uint64_t extent_id) const {
        auto segment_path = SegmentSet::segment_path(_file, SegmentSet::segment_id_of(extent_id));
        return _source->get(_database_id, segment_path, SegmentSet::offset_of(extent_id), _xid);
    }

    void GistReader::_collect_all(uint64_t extent_id, std::vector<GistEntry> &out) const {
        auto extent = _get_extent(extent_id);
        bool is_branch = extent->header().type.is_branch();

        if (!is_branch) {
            for (auto it = extent->begin(); it != extent->end(); ++it) {
                out.push_back(_strategy.read_leaf(*it, _leaf_schema));
            }
            return;
        }

        for (auto it = extent->begin(); it != extent->end(); ++it) {
            auto entry = _strategy.read_branch(*it, _branch_schema);
            _collect_all(entry.child_id, out);
        }
    }

    std::vector<GistEntry> GistReader::all_leaves() const {
        std::vector<GistEntry> out;
        if (_root_offset != constant::UNKNOWN_EXTENT) {
            _collect_all(_root_offset, out);
        }
        return out;
    }

    bool GistReader::empty() const {
        if (_root_offset == constant::UNKNOWN_EXTENT) {
            return true;
        }
        return _get_extent(_root_offset)->empty();
    }
}
