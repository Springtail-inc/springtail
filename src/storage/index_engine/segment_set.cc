#include <absl/log/check.h>

#include <common/json.hh>
#include <common/properties.hh>
#include <storage/index_engine/segment_set.hh>
#include <storage/vacuumer.hh>

namespace springtail {
    namespace {
        uint64_t
        file_size_or_zero(const std::filesystem::path &path)
        {
            std::error_code ec;
            uint64_t size = std::filesystem::file_size(path, ec);
            return ec ? 0 : size;
        }
    }

    uint64_t
    SegmentSet::default_seal_threshold_bytes()
    {
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        return Json::get_or<uint64_t>(json, "segment_seal_threshold_bytes",
                                      DEFAULT_SEAL_THRESHOLD_BYTES);
    }

    SegmentSet::SegmentSet(std::filesystem::path base_file, uint64_t seal_threshold_bytes)
        : _base_file(std::move(base_file)),
          _seal_threshold_bytes(seal_threshold_bytes)
    { }

    void
    SegmentSet::resume_from(uint64_t real_id)
    {
        _current_segment_id = segment_id_of(real_id);
        _maybe_seal_and_rotate();
    }

    void
    SegmentSet::advance_after_flush()
    {
        _maybe_seal_and_rotate();
    }

    void
    SegmentSet::_maybe_seal_and_rotate()
    {
        auto path = current_path();
        if (file_size_or_zero(path) < _seal_threshold_bytes) {
            return;
        }

        // Idempotent -- re-registering an already-sealed segment (e.g. every time a later
        // generation resumes into it) is harmless, just re-affirms membership (see the class
        // comment on why "sealed" is always re-derived rather than trusted from a prior run).
        Vacuumer::get_instance()->register_sealed_segment(path);

        DCHECK_LT(_current_segment_id, MAX_SEGMENT_ID) << "segment_id space exhausted for tree "
                                                        << _base_file.native();
        ++_current_segment_id;
    }

    std::vector<std::filesystem::path>
    SegmentSet::all_paths() const
    {
        std::vector<std::filesystem::path> paths;
        for (uint32_t i = 0; i <= _current_segment_id; i++) {
            auto path = segment_path(_base_file, i);
            if (std::filesystem::exists(path)) {
                paths.push_back(std::move(path));
            }
        }
        return paths;
    }
}
