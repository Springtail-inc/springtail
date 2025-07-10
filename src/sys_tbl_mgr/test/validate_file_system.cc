#include <fmt/ranges.h>

#include <common/init.hh>
#include <common/constants.hh>
#include <common/json.hh>

#include <storage/field.hh>
#include <storage/io_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/table_mgr.hh>

using namespace springtail;

int
main(int argc,
     char *argv[])
{
    // no logging
    springtail_init(false, std::nullopt, LOG_NONE);

    // get all database ids
    std::map<uint64_t, std::string> databases = Properties::get_databases();

    // get base path to database files
    std::filesystem::path table_base;
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "table_dir", table_base);
    table_base = Properties::make_absolute_path(table_base);
    std::cout << fmt::format("\ntable_base = {}", table_base.string()) << std::endl;


    // iterate over databases
    for (const auto &db_id_name: databases) {
        uint64_t db_id = db_id_name.first;
        const std::string &db_name = db_id_name.second;

        // read table names table
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, constant::LATEST_XID);
        auto table_fields = table->extent_schema()->get_fields();

        for (auto row: (*table)) {
            auto table_ns_id = table_fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);
            std::string table_name(table_fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));
            uint64_t tid = table_fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
            uint64_t xid = table_fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);
            bool exists = table_fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
            if (!exists) {
                std::cout << fmt::format("\nDB {}:{}: Skipping non-existing table {}.{} tid={}, xid={}",
                    db_id, db_name, table_ns_id, table_name, tid, xid) << std::endl;
                continue;
            }
            std::cout << fmt::format("\nDB {}:{}: Found table {}.{} tid={}, xid={}",
                    db_id, db_name, table_ns_id, table_name, tid, xid) << std::endl;

            // read table stats table
            auto table_stats = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableStats::ID, constant::LATEST_XID);
            auto table_stats_fields = table_stats->extent_schema()->get_fields();
            auto table_stats_search_key = sys_tbl::TableStats::Primary::key_tuple(tid, constant::LATEST_XID);
            auto table_stats_iter = table_stats->inverse_lower_bound(table_stats_search_key);
            if (table_stats_iter != table_stats->end()) {
                auto &table_stats_row = *table_stats_iter;
                uint64_t stats_tid = table_stats_fields->at(sys_tbl::TableStats::Data::TABLE_ID)->get_uint64(&table_stats_row);
                uint64_t stats_xid = table_stats_fields->at(sys_tbl::TableStats::Data::XID)->get_uint64(&table_stats_row);
                uint64_t row_count = table_stats_fields->at(sys_tbl::TableStats::Data::ROW_COUNT)->get_uint64(&table_stats_row);
                uint64_t end_offset = table_stats_fields->at(sys_tbl::TableStats::Data::END_OFFSET)->get_uint64(&table_stats_row);
                std::cout << fmt::format("DB {}:{}: Table {}.{} stats data: tid={}, xid={}, row_count={}, end_offset={}",
                    db_id, db_name, table_ns_id, table_name, stats_tid, stats_xid,row_count, end_offset) << std::endl;
            } else {
                std::cout << fmt::format("DB {}:{}: Table {}.{} stats data: no entry found",
                    db_id, db_name, table_ns_id, table_name) << std::endl;
            }

            // read table roots table
            auto table_roots = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableRoots::ID, constant::LATEST_XID);
            auto table_roots_fields = table_roots->extent_schema()->get_fields();

            auto table_roots_search_key = sys_tbl::TableRoots::Primary::key_tuple(tid, 0, xid);
            for (auto table_roots_iter = table_roots->lower_bound(table_roots_search_key);
                 table_roots_iter != table_roots->end();
                 ++table_roots_iter) {
                auto &table_roots_row = *table_roots_iter;
                uint64_t roots_tid = table_roots_fields->at(sys_tbl::TableRoots::Data::TABLE_ID)->get_uint64(&table_roots_row);
                uint64_t roots_iid = table_roots_fields->at(sys_tbl::TableRoots::Data::INDEX_ID)->get_uint64(&table_roots_row);
                uint64_t roots_xid = table_roots_fields->at(sys_tbl::TableRoots::Data::XID)->get_uint64(&table_roots_row);
                uint64_t roots_eid = table_roots_fields->at(sys_tbl::TableRoots::Data::EXTENT_ID)->get_uint64(&table_roots_row);
                uint64_t roots_sxid = table_roots_fields->at(sys_tbl::TableRoots::Data::SNAPSHOT_XID)->get_uint64(&table_roots_row);
                if (roots_tid == tid) {
                    // do not look at entries with invalid xid
                    if (roots_eid == constant::UNKNOWN_EXTENT) {
                        continue;
                    }
                    std::cout << fmt::format("DB {}:{}: Table {}.{} roots data: tid={}, iid={}, xid={}, eid={}, sxid={}",
                        db_id, db_name, table_ns_id, table_name, roots_tid, roots_iid, roots_xid, roots_eid, roots_sxid) << std::endl;
                    std::cout << fmt::format("File path: {}/{}/{}-{}/raw",
                        table_base.string(), db_id, roots_tid, roots_sxid) << std::endl;

                    // read database table extent
                    TableMetadata table_metadata;
                    table_metadata.roots.push_back(TableRoot{roots_iid, roots_eid});
                    table_metadata.snapshot_xid = roots_sxid;
                    table_metadata.stats = {};

                    std::vector<std::string> empty_primary_key;
                    std::vector<Index> empty_secondary_indexes;

                    auto data_table = std::make_shared<Table>(db_id, roots_tid, roots_xid, table_base,
                        empty_primary_key, empty_secondary_indexes, table_metadata, nullptr);

                    while (true) {
                        auto data_table_extent = data_table->read_extent_from_disk(roots_eid);
                        if (data_table_extent.first == nullptr && data_table_extent.second == 0) {
                            std::cout << fmt::format("DB {}:{}: Table {}.{}, Extent {}: empty",
                                db_id, db_name, table_ns_id, table_name, roots_eid) << std::endl;
                            break;
                        } else {
                            auto table_extent = data_table_extent.first;
                            std::cout << fmt::format("DB {}:{}: Table {}.{}, Extent {}: not empty, next offset={}",
                                db_id, db_name, table_ns_id, table_name, roots_eid, data_table_extent.second) << std::endl;
                            if (table_extent->empty()) {
                                std::cout << fmt::format("DB {}:{}: Table {}.{}, Extent {}: empty extent",
                                    db_id, db_name, table_ns_id, table_name, roots_eid) << std::endl;
                            } else {
                                std::cout << fmt::format("DB {}:{}: Table {}.{}, Extent {}: row size = {}, row count = {}",
                                    db_id, db_name, table_ns_id, table_name, roots_eid, table_extent->row_size(), table_extent->row_count()) << std::endl;
                            }
                            if (data_table_extent.second < roots_eid) {
                                break;
                            }
                            roots_eid = data_table_extent.second;
                        }
                    }
                } else if (roots_tid > tid) {
                    break;
                } else {
                    std::cout << fmt::format("DB {}:{}: Table {}.{} roots data: tid={}, iid={}, xid={}, eid={}, sxid={}",
                        db_id, db_name, table_ns_id, table_name, roots_tid, roots_iid, roots_xid, roots_eid, roots_sxid) << std::endl;
                }
            }
        }
    }
    std::cout << std::endl;
    springtail_shutdown();
}
