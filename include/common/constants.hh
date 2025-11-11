#pragma once

#include <string>
#include <limits>
#include <cstdint>
#include <span>

namespace springtail {
    struct ExtensionContext {
        uint64_t db_id;
        uint64_t xid;
        int32_t type_oid;
        const char* op_str;
    };

    struct ExtensionCallback {
        using ComparatorFunc = bool (*)(const ExtensionContext*,
                                        const std::span<const char>&,
                                        const std::span<const char>&);

        ComparatorFunc comparator_func = nullptr;
        ExtensionContext context = {};
    };

    // GIST Proc
    constexpr int GIST_CONSISTENT = 1;
    constexpr int GIST_UNION = 2;
    constexpr int GIST_COMPRESS = 3;
    constexpr int GIST_DECOMPRESS = 4;
    constexpr int GIST_PENALTY = 5;
    constexpr int GIST_PICKSPLIT = 6;
    constexpr int GIST_EQUAL = 7;
    constexpr int GIST_DISTANCE = 8;
    constexpr int GIST_FETCH = 9;
    constexpr int GIST_OPTIONS = 10;
    constexpr int GIST_SORTSUPPORT = 11;

    // GIN Proc
    constexpr int GIN_COMPARE = 1;
    constexpr int GIN_EXTRACTVALUE = 2;
    constexpr int GIN_EXTRACTQUERY = 3;
    constexpr int GIN_CONSISTENT = 4;
    constexpr int GIN_COMPARE_PARTIAL = 5;
    constexpr int GIN_TRICONSISTENT = 6;
    constexpr int GIN_OPTIONS = 7;
}

namespace springtail::constant {
    /** Used as an extent ID in situations where the extent ID is unknown. */
    static constexpr uint64_t UNKNOWN_EXTENT = std::numeric_limits<uint64_t>::max();

    /** Used as namespace id when namespace id is unknown. */
    static constexpr uint64_t MAX_NAMESPACE_ID = std::numeric_limits<uint64_t>::max();

    /** Represents the most recent XID available. */
    static constexpr uint64_t LATEST_XID = std::numeric_limits<uint64_t>::max();

    /** Invalid XID value */
    static constexpr uint64_t INVALID_XID = 0;

    /** Represents the most recent LSN available. */
    static constexpr uint64_t MAX_LSN = std::numeric_limits<uint64_t>::max();

    /** XXX: Represents some specific LSNs to maintain ordering within the XID */

    static constexpr uint64_t INDEX_COMMIT_LSN = MAX_LSN - 3;
    static constexpr uint64_t RESYNC_NAMESPACE_LSN = MAX_LSN - 2;
    static constexpr uint64_t RESYNC_DROP_LSN = MAX_LSN - 2;
    static constexpr uint64_t RESYNC_CREATE_LSN = MAX_LSN - 1;

    /** Represents an invalid database ID. */
    static constexpr uint64_t INVALID_DB_ID = 0;

    /** Represents an invalid table ID. */
    static constexpr uint64_t INVALID_TABLE = 0;

    /** Represents the table ID cut-off for system tables. */
    static constexpr uint64_t MAX_SYSTEM_TABLE_ID = 512;

    /** The target maximum extent size for data and primary indexes. */
    static constexpr uint64_t MAX_EXTENT_SIZE = 64 * 1024;

    /** The target maximum extent size for secondary indexes. */
    static constexpr uint64_t MAX_EXTENT_SIZE_SECONDARY = 64 * 1024;

    /** An index ID that represents an extent containing raw data rather than index data.  Used in
        the extent header. */
    static constexpr uint32_t INDEX_DATA = std::numeric_limits<uint32_t>::max();

    /** An index ID that represents the primary index. */
    static constexpr uint64_t INDEX_PRIMARY = 0;

    /** An index ID that represents the look-aside index. */
    static constexpr uint64_t INDEX_LOOK_ASIDE = std::numeric_limits<uint64_t>::max();

    /** The name of the child pointer field in a BTree branch extent. */
    static const std::string BTREE_CHILD_FIELD = "__springtail_child";

    /** The name of the extent ID field in an index leaf extent. */
    static const std::string INDEX_EID_FIELD = "__springtail_eid";

    /** The name of the row ID field in a secondary index leaf extent. */
    static const std::string INDEX_RID_FIELD = "__springtail_rid";

    /** The format of an index file name. */
    static constexpr std::string_view INDEX_FILE = "{}.idx";
    static constexpr std::string_view INDEX_PRIMARY_FILE = "0.idx";
    static constexpr std::string_view INDEX_LOOK_ASIDE_FILE = "look_aside.idx";

    /**
     * Name of the internal row ID for the rows in the table,
     * to be used in the look-aside index for secondary indexes
     */
    static const std::string INTERNAL_ROW_ID = "__springtail_internal_row_id";

    /**
     * Different types of secondary indexes
     */
    static constexpr std::string_view INDEX_TYPE_GIN = "gin";
    static constexpr std::string_view INDEX_TYPE_BTREE = "btree";

    /** The format of a raw data file name. */
    static constexpr std::string_view DATA_FILE = "raw";

    /** The format of a roots file. */
    static constexpr std::string_view ROOTS_FILE = "roots";
    static constexpr std::string_view ROOTS_XID_FILE = "roots.{}";
    static constexpr std::string_view ROOTS_TMP_FILE = "roots.tmp";

    /** Coordinator keep alive timeout seconds */
    static constexpr uint32_t COORDINATOR_KEEP_ALIVE_TIMEOUT = 5;

    /** First User defined PG OID - FirstNormalObjectId in postgres include/access/transam.h */
    static constexpr uint32_t FIRST_USER_DEFINED_PG_OID = 16384;

    /** User defined types */
    static constexpr int8_t USER_TYPE_ENUM = 'E';
    static constexpr int8_t USER_TYPE_EXTENSION = 'U';
}
