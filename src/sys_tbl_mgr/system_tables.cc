#include <common/constants.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <postgresql/server/catalog/pg_type_d.h>

// note: these are hard-coded from the postgres type OIDs to avoid having to include all of the
// postgres headers here

namespace springtail::sys_tbl {

// TableNames
// NOTE: position is 1 based for Postgres compatibility
const std::vector<SchemaColumn> TableNames::Data::SCHEMA = {
    {"namespace_id", 1, SchemaType::UINT64, INT8OID, false},
    {"name", 2, SchemaType::TEXT, TEXTOID, false},
    {"table_id", 3, SchemaType::UINT64, INT8OID, false, 0},
    {"xid", 4, SchemaType::UINT64, INT8OID, false, 1},
    {"lsn", 5, SchemaType::UINT64, INT8OID, false, 2},
    {"exists", 6, SchemaType::BOOLEAN, BOOLOID, false},
    {"parent_table_id", 7, SchemaType::UINT64, INT8OID, true},
    {"partition_key", 8, SchemaType::TEXT, TEXTOID, true},
    {"partition_bound", 9, SchemaType::TEXT, TEXTOID, true},
    {"rls_enabled", 10, SchemaType::BOOLEAN, BOOLOID, false},
    {"rls_forced", 11, SchemaType::BOOLEAN, BOOLOID, false}};

const std::vector<SchemaColumn> TableNames::Primary::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false},
    {"xid", 2, SchemaType::UINT64, INT8OID, false},
    {"lsn", 3, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> TableNames::Primary::KEY = {"table_id", "xid", "lsn"};

const std::vector<SchemaColumn> TableNames::Secondary::SCHEMA = {
    {"namespace_id", 1, SchemaType::UINT64, INT8OID, false},
    {"name", 2, SchemaType::TEXT, TEXTOID, false},
    {"xid", 3, SchemaType::UINT64, INT8OID, false},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 5, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_RID_FIELD, 6, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> TableNames::Secondary::KEY = {"namespace_id", "name", "xid", "lsn"};

// TableRoots
const std::vector<SchemaColumn> TableRoots::Data::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false, 0},
    {"index_id", 2, SchemaType::UINT64, INT8OID, false, 1},
    {"xid", 3, SchemaType::UINT64, INT8OID, false, 2},
    {"extent_id", 4, SchemaType::UINT64, INT8OID, false},
    {"snapshot_xid", 5, SchemaType::UINT64, INT8OID, false},
    {"end_offset", 6, SchemaType::UINT64, INT8OID, false}};

const std::vector<SchemaColumn> TableRoots::Primary::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false},
    {"index_id", 2, SchemaType::UINT64, INT8OID, false},
    {"xid", 3, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> TableRoots::Primary::KEY = {"table_id", "index_id", "xid"};

// Indexes
const std::vector<SchemaColumn> Indexes::Data::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false, 0},
    {"index_id", 2, SchemaType::UINT64, INT8OID, false, 1},
    {"xid", 3, SchemaType::UINT64, INT8OID, false, 2},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false, 3},
    {"position", 5, SchemaType::UINT32, INT4OID, false, 4},
    {"column_id", 6, SchemaType::UINT32, INT4OID, false},
};

const std::vector<SchemaColumn> Indexes::Primary::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false},
    {"index_id", 2, SchemaType::UINT64, INT8OID, false},
    {"xid", 3, SchemaType::UINT64, INT8OID, false},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false},
    {"position", 5, SchemaType::UINT32, INT4OID, false},
    {constant::INDEX_EID_FIELD, 6, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> Indexes::Primary::KEY = {"table_id", "index_id", "xid", "lsn",
                                                        "position"};

// Schemas
const std::vector<SchemaColumn> Schemas::Data::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false, 0},
    {"position", 2, SchemaType::UINT32, INT4OID, false, 1},
    {"xid", 3, SchemaType::UINT64, INT8OID, false, 2},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false, 3},
    {"exists", 5, SchemaType::BOOLEAN, BOOLOID, false},
    {"name", 6, SchemaType::TEXT, TEXTOID, false},
    {"type", 7, SchemaType::UINT8, INT8OID, false},
    {"pg_type", 8, SchemaType::INT32, INT4OID, false},
    {"nullable", 9, SchemaType::BOOLEAN, BOOLOID, false},
    {"default", 10, SchemaType::TEXT, TEXTOID, true},
    {"update_type", 11, SchemaType::UINT8, INT8OID, false}};

const std::vector<SchemaColumn> Schemas::Primary::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false},
    {"position", 2, SchemaType::UINT32, INT4OID, false},
    {"xid", 3, SchemaType::UINT64, INT8OID, false},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 5, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> Schemas::Primary::KEY = {"table_id", "position", "xid", "lsn"};

// TableStats
const std::vector<SchemaColumn> TableStats::Data::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false, 0},
    {"xid", 2, SchemaType::UINT64, INT8OID, false, 1},
    {"row_count", 3, SchemaType::UINT64, INT8OID, false}};

const std::vector<SchemaColumn> TableStats::Primary::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false},
    {"xid", 2, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 3, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> TableStats::Primary::KEY = {"table_id", "xid"};

// IndexNames
const std::vector<SchemaColumn> IndexNames::Data::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false, 0},
    {"index_id", 2, SchemaType::UINT64, INT8OID, false, 1},
    {"xid", 3, SchemaType::UINT64, INT8OID, false, 2},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false, 3},
    {"namespace_id", 5, SchemaType::UINT64, INT8OID, false},
    {"name", 6, SchemaType::TEXT, TEXTOID, false},
    {"state", 7, SchemaType::UINT8, INT8OID, false},
    {"is_unique", 8, SchemaType::BOOLEAN, BOOLOID, false}};

const std::vector<SchemaColumn> IndexNames::Primary::SCHEMA = {
    {"table_id", 1, SchemaType::UINT64, INT8OID, false},
    {"index_id", 2, SchemaType::UINT64, INT8OID, false},
    {"xid", 3, SchemaType::UINT64, INT8OID, false},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false},
};

const std::vector<std::string> IndexNames::Primary::KEY = {"table_id", "index_id", "xid", "lsn"};

// NamespaceNames
const std::vector<SchemaColumn> NamespaceNames::Data::SCHEMA = {
    {"namespace_id", 1, SchemaType::UINT64, INT8OID, false, 0},
    {"name", 2, SchemaType::TEXT, TEXTOID, false},
    {"xid", 3, SchemaType::UINT64, INT8OID, false, 1},
    {"lsn", 4, SchemaType::UINT64, INT8OID, false, 2},
    {"exists", 5, SchemaType::BOOLEAN, BOOLOID, false}};

const std::vector<SchemaColumn> NamespaceNames::Primary::SCHEMA = {
    {"namespace_id", 1, SchemaType::UINT64, INT8OID, false},
    {"xid", 2, SchemaType::UINT64, INT8OID, false},
    {"lsn", 3, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> NamespaceNames::Primary::KEY = {"namespace_id", "xid", "lsn"};

const std::vector<SchemaColumn> NamespaceNames::Secondary::SCHEMA = {
    {"name", 1, SchemaType::TEXT, TEXTOID, false},
    {"xid", 2, SchemaType::UINT64, INT8OID, false},
    {"lsn", 3, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_RID_FIELD, 5, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> NamespaceNames::Secondary::KEY = {"name", "xid", "lsn"};

// User defined types
const std::vector<SchemaColumn> UserTypes::Data::SCHEMA = {
    {"type_id", 1, SchemaType::UINT64, INT8OID, false, 0},
    {"namespace_id", 2, SchemaType::UINT64, INT8OID, false},
    {"name", 3, SchemaType::TEXT, TEXTOID, false},
    {"value", 4, SchemaType::TEXT, TEXTOID, false},
    {"xid", 5, SchemaType::UINT64, INT8OID, false, 1},
    {"lsn", 6, SchemaType::UINT64, INT8OID, false, 2},
    {"type", 7, SchemaType::UINT8, CHAROID, false},
    {"exists", 8, SchemaType::BOOLEAN, BOOLOID, false}};

const std::vector<SchemaColumn> UserTypes::Primary::SCHEMA = {
    {"type_id", 1, SchemaType::UINT64, INT8OID, false},
    {"xid", 2, SchemaType::UINT64, INT8OID, false},
    {"lsn", 3, SchemaType::UINT64, INT8OID, false},
    {constant::INDEX_EID_FIELD, 4, SchemaType::UINT64, INT8OID, false}};

const std::vector<std::string> UserTypes::Primary::KEY = {"type_id", "xid", "lsn"};

}  // namespace springtail::sys_tbl
