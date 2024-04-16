#include <cstdlib>
#include <sstream>
#include <variant>
#include <algorithm>

#include <fmt/core.h>
#include <nlohmann/json.hpp>

#include <common/logging.hh>
#include <common/common.hh>

#include <pg_repl/exception.hh>
#include <pg_repl/pg_repl_msg.hh>

namespace springtail {
namespace pg_msg {

    void
    dump_tuple(const PgMsgTupleData &tuple,
               std::stringstream &ss) noexcept
    {
        for (auto && c: tuple.tuple_data) {
            ss << "  - type=" << c.type << std::endl;
            ss << "  - data_len=" << c.data.size() << std::endl;
        }
    }

    /**
     * @brief Convert LSN to string of format XXX/XXX
     *
     * @param lsn LSN to convert
     * @return string of LSN in format: "XXX/XXX"
     */
    std::string
    lsn_to_str(const LSN_t lsn) noexcept
    {
        uint32_t lsn_higher = static_cast<uint32_t>(lsn>>32);
        uint32_t lsn_lower = static_cast<uint32_t>(lsn);

        return fmt::format("{:X}/{:X}", lsn_higher, lsn_lower);
    }

    /**
     * @brief Convert LSN in string format XXX/XXX to LSN_t (uint64_t)
     *
     * @param lsn_str string of LSN in format XXX/XXX
     * @return LSN_t
     */
    LSN_t
    str_to_LSN(const char *lsn_str) noexcept
    {
        char *end_ptr = nullptr;

        if (lsn_str == nullptr) {
            return INVALID_LSN;
        }

        // convert high bits
        uint64_t lsn_higher = strtol(lsn_str, &end_ptr, 16);

        // end_ptr now points to the '/' -- validate
        if (end_ptr == nullptr || *end_ptr != '/') {
            return INVALID_LSN;
        }

        // convert low bits starting at end_ptr + 1
        uint64_t lsn_lower = strtol(end_ptr+1, nullptr, 16);

        return (lsn_higher << 32) | (0xFFFFFFFF & lsn_lower);
    }

    /**
     * @brief convert a message to a printable string
     *
     * @param msg refernece to message to convert
     * @return readable string of msg
     */
    std::string
    dump_msg(const PgMsg &msg)
    {
        std::stringstream ss;

        switch(msg.msg_type) {
            case BEGIN: {
                PgMsgBegin begin = std::get<PgMsgBegin>(msg.msg);
                ss << "\nBEGIN" << std::endl;
                ss << "  xid=" << begin.xid << std::endl;
                ss << "  LSN=" << begin.xact_lsn << " ("
                   << lsn_to_str(begin.xact_lsn) << ")\n";
                break;
            }

            case COMMIT: {
                PgMsgCommit commit = std::get<PgMsgCommit>(msg.msg);
                ss << "\nCOMMIT" << std::endl;
                ss << "  commit LSN=" << commit.commit_lsn
                   << " (" << lsn_to_str(commit.commit_lsn) << ")\n";
                ss << "  xact LSN=" << commit.xact_lsn
                   << " (" << lsn_to_str(commit.xact_lsn) << ")\n";
                break;
            }

            case RELATION: {
                PgMsgRelation relation = std::get<PgMsgRelation>(msg.msg);
                ss << "\nRELATION" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << relation.xid << std::endl;
                }
                ss << "  rel_id=" << relation.rel_id << std::endl;
                ss << "  namespace=" << relation.namespace_str << std::endl;
                ss << "  rel_name=" << relation.rel_name_str << std::endl;

                ss << "  Columns" << std::endl;
                for (std::size_t i = 0; i < relation.columns.size(); i++) {
                    ss << "  - name=" << relation.columns[i].column_name << std::endl;
                    ss << "  - key=" << (relation.columns[i].flags == 1) << std::endl;
                    ss << "  - oid=" << relation.columns[i].oid << std::endl;
                    ss << "  - type modifier=" << relation.columns[i].type_modifier << std::endl;
                }
                break;
            }

            case INSERT: {
                PgMsgInsert insert = std::get<PgMsgInsert>(msg.msg);
                ss << "\nINSERT" << " (" << msg.proto_version << ")" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << insert.xid << std::endl;
                }
                ss << "  rel_id=" << insert.rel_id << std::endl;
                ss << "  New tuples" << std::endl;
                dump_tuple(insert.new_tuple, ss);
                break;
            }

            case DELETE: {
                PgMsgDelete delete_msg = std::get<PgMsgDelete>(msg.msg);
                ss << "\nDELETE";
                if (msg.is_streaming) {
                    ss << "  xid=" << delete_msg.xid << std::endl;
                }
                ss << "  rel_id=" << delete_msg.rel_id << std::endl;
                ss << "  Tuples (" << delete_msg.type << ")\n";
                dump_tuple(delete_msg.tuple, ss);
                break;
            }

            case UPDATE: {
                PgMsgUpdate update = std::get<PgMsgUpdate>(msg.msg);
                ss << "\nUPDATE";
                if (msg.is_streaming) {
                    ss << "  xid=" << update.xid << std::endl;
                }
                ss << "  rel_id=" << update.rel_id << std::endl;
                ss << "  Old tuples (" << update.old_type << ")" << std::endl;
                dump_tuple(update.old_tuple, ss);
                ss << "  New tuples (" << update.new_type << ")" << std::endl;
                dump_tuple(update.new_tuple, ss);
                break;
            }

            case TRUNCATE: {
                PgMsgTruncate truncate = std::get<PgMsgTruncate>(msg.msg);
                ss << "\nTRUNCATE" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << truncate.xid << std::endl;
                }
                for (int32_t rel_id: truncate.rel_ids) {
                    ss << "  rel_id=" << rel_id << std::endl;
                }
                break;
            }

            case ORIGIN: {
                PgMsgOrigin origin = std::get<PgMsgOrigin>(msg.msg);
                ss << "\nORIGIN" << std::endl;
                ss << "  commit LSN=" << origin.commit_lsn
                   << " (" << lsn_to_str(origin.commit_lsn) << ")\n";
                ss << "  name=" << origin.name_str << std::endl;
                break;
            }

            case MESSAGE: {
                PgMsgMessage message = std::get<PgMsgMessage>(msg.msg);
                ss << "\nMESSAGE" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << message.xid << std::endl;
                }
                ss << "  LSN=" << message.lsn
                   << " (" << lsn_to_str(message.lsn) << ")\n";
                ss << "  prefix=" << message.prefix_str << std::endl;
                break;
            }

            case TYPE: {
                PgMsgType type = std::get<PgMsgType>(msg.msg);
                ss << "\nTYPE" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << type.xid << std::endl;
                }
                ss << "  oid=" << type.oid << std::endl;
                ss << "  namespace=" << type.namespace_str << std::endl;
                ss << "  data type=" << type.data_type_str << std::endl;
                break;
            }

            case STREAM_START: {
                PgMsgStreamStart start = std::get<PgMsgStreamStart>(msg.msg);
                ss << "\nSTREAM START" << std::endl;
                ss << "  xid=" << start.xid << std::endl;
                ss << "  first=" << start.first << std::endl;
                break;
            }

            case STREAM_STOP: {
                ss << "\nSTREAM STOP" << std::endl;
                break;
            }

            case STREAM_COMMIT: {
                PgMsgStreamCommit commit = std::get<PgMsgStreamCommit>(msg.msg);
                ss << "\nSTREAM COMMIT" << std::endl;
                ss << "  xid=" << commit.xid << std::endl;
                ss << "  commit LSN=" << commit.commit_lsn
                   << " (" << lsn_to_str(commit.commit_lsn) << ")\n";
                ss << "  xact LSN=" << commit.xact_lsn
                   << " (" << lsn_to_str(commit.xact_lsn) << ")\n";
                break;
            }

            case STREAM_ABORT: {
                PgMsgStreamAbort abort = std::get<PgMsgStreamAbort>(msg.msg);
                ss << "\nSTREAM ABORT" << std::endl;
                ss << "  xid=" << abort.xid << std::endl;
                ss << "  sub_xid=" << abort.sub_xid << std::endl;
                break;
            }

            case CREATE_TABLE: {
                PgMsgTable table = std::get<PgMsgTable>(msg.msg);
                ss << "\nCREATE TABLE" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << table.xid << std::endl;
                }
                ss << "  oid=" << table.oid << std::endl;
                ss << "  LSN=" << table.lsn << std::endl;
                ss << "  schema=" << table.schema << std::endl;
                ss << "  table=" << table.table << std::endl;
                ss << "  columns=" << table.columns.size() << std::endl;

                for (PgMsgSchemaColumn &column: table.columns) {
                    ss << "  - name=" << column.column_name << std::endl;
                    ss << "  - type=" << column.udt_type << std::endl;
                    ss << "  - default=" << column.default_value.value_or("NULL") << std::endl;
                    ss << "  - is_nullable=" << column.is_nullable << std::endl;
                    ss << "  - is_pkey=" << column.is_pkey << std::endl;
                    ss << "  - position=" << column.position << std::endl;
                }

                break;
            }

            case ALTER_TABLE: {
                PgMsgTable table = std::get<PgMsgTable>(msg.msg);
                ss << "\nALTER TABLE" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << table.xid << std::endl;
                }
                ss << "  oid=" << table.oid << std::endl;
                ss << "  LSN=" << table.lsn << std::endl;
                ss << "  schema=" << table.schema << std::endl;
                ss << "  table=" << table.table << std::endl;
                ss << "  columns=" << table.columns.size() << std::endl;

                for (PgMsgSchemaColumn &column: table.columns) {
                    ss << "  - name=" << column.column_name << std::endl;
                    ss << "  - type=" << column.udt_type << std::endl;
                    ss << "  - default=" << column.default_value.value_or("NULL") << std::endl;
                    ss << "  - is_nullable=" << column.is_nullable << std::endl;
                    ss << "  - position=" << column.position << std::endl;
                }

                break;
            }

            case DROP_TABLE: {
                PgMsgDropTable drop_table = std::get<PgMsgDropTable>(msg.msg);
                ss << "\nDROP TABLE" << std::endl;
                if (msg.is_streaming) {
                    ss << "  xid=" << drop_table.xid << std::endl;
                }
                ss << "  oid=" << drop_table.oid << std::endl;
                ss << "  LSN=" << drop_table.lsn << std::endl;
                ss << "  schema=" << drop_table.schema << std::endl;
                ss << "  table=" << drop_table.table << std::endl;
                break;
            }

            default:
                break;
        }

        return ss.str();
    }
} // namespace pg_repl_msg
} // namespace springtail
