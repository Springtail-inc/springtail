#pragma once

#include <string>
#include <limits>

namespace springtail::constant {
    /** Used as an extent ID in situations where the extent ID is unknown. */
    static constexpr uint64_t UNKNOWN_EXTENT = std::numeric_limits<uint64_t>::max();

    /** Represents the most recent XID available. */
    static constexpr uint64_t LATEST_XID = std::numeric_limits<uint64_t>::max();

    /** Represents the most recent LSN available. */
    static constexpr uint64_t MAX_LSN = std::numeric_limits<uint64_t>::max();

    /** Represents an invalid table ID. */
    static constexpr uint64_t INVALID_TABLE = 0;

    /** The target maximum extent size. */
    static constexpr uint64_t MAX_EXTENT_SIZE = 64 * 1024;

    /** An index ID that represents an extent containing raw data rather than index data.  Used in
        the extent header. */
    static constexpr uint32_t INDEX_DATA = std::numeric_limits<uint32_t>::max();

    /** An index ID that represents the primary index. */
    static constexpr uint32_t INDEX_PRIMARY = 0;

    /** The name of the child pointer field in a BTree branch extent. */
    static const std::string BTREE_CHILD_FIELD = "__springtail_child";

    /** The name of the extent ID field in an index leaf extent. */
    static const std::string INDEX_EID_FIELD = "__springtail_eid";

    /** The name of the row ID field in a secondary index leaf extent. */
    static const std::string INDEX_RID_FIELD = "__springtail_rid";

    /** The format of an index file name. */
    static constexpr std::string_view INDEX_FILE = "{}.idx";
    static constexpr std::string_view INDEX_PRIMARY_FILE = "0.idx";

    /** The format of a raw data file name. */
    static constexpr std::string_view DATA_FILE = "raw";

    /** The format of a roots file. */
    static constexpr std::string_view ROOTS_FILE = "roots";
    static constexpr std::string_view ROOTS_XID_FILE = "roots.{}";
    static constexpr std::string_view ROOTS_TMP_FILE = "roots.tmp";

}
