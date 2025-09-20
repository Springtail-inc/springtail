#include <common/constants.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <sys_tbl_mgr/table_mgr_base.hh>

namespace springtail {
    TableMgrBase::TableMgrBase()
    {
        // get the base directory for table data
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<std::filesystem::path>(json, "table_dir", _table_base);
        _table_base = Properties::make_absolute_path(_table_base);
    }

    std::map<uint32_t, SchemaColumn>
    TableMgrBase::_convert_columns(const std::vector<SchemaColumn> &columns)
    {
        std::map<uint32_t, SchemaColumn> column_map;
        for (auto &&column : columns) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "col_pos={} col_name={}",
                        column.position, column.name);
            column_map.insert({column.position, column});
        }
        return column_map;
    }

} // springtail