#pragma once

#include <memory>
#include <filesystem>
#include <map>
#include <set>

#include <iostream>

namespace springtail {
    /**
     * @brief Postgres log manager singleton
     * Responsible for storing Postgres logs
     */
    class PgLogMgr {
    public:

        PgLogMgr(const PgLogMgr&) = delete;
        PgLogMgr& operator=(const PgLogMgr &) = delete;
        PgLogMgr& operator=(const PgLogMgr &&) = delete;

        /** get singleton instance */
        static PgLogMgr *get_instance() {
            std::call_once(_init_flag, &PgLogMgr::init);
            return _instance;
        }

    private:
        struct XactEntry {
            std::filesystem::path path;
            uint64_t start_offset;
            uint64_t end_offset;
        };
        using XactEntryPtr = std::shared_ptr<XactEntry>;

        PgLogMgr();

        /** initializer for singleton, called once */
        static void init();

        /** singleton instance */
        static PgLogMgr *_instance;

        /** once flag for once initialization */
        static std::once_flag _init_flag;

        /** base path */
        std::filesystem::path _base_path;

        /** current file path */
        std::filesystem_path _current_file;

        /** map from file to set of non-committed pgxids for that file */
        std::map<std::filesystem::path, std::set<uint64_t>> _inflight_file_map;

        /** map from pgxid to transaction entry */
        std::map<uint64_t, XactEntryPtr> _inflight_xact_map;
    };

} // namespace springtail