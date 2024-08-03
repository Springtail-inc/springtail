#include <cstdio>
#include <cstring>
#include <cassert>
#include <vector>
#include <algorithm>

#include <fmt/core.h>

// springtail includes
#include <common/common.hh>
#include <pg_repl/exception.hh>
#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_copy_table.hh>
#include <pg_repl/libpq_connection.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <storage/system_tables.hh>
#include <storage/schema.hh>
#include <storage/table.hh>
#include <storage/table_mgr.hh>

extern "C" {
    #include <postgres.h>
    #include <catalog/pg_type.h>
}

/* See: https://www.postgresql.org/docs/current/datatype.html for postgres types */

namespace springtail
{
    /** get oid for table */
    static constexpr char TABLE_OID_QUERY[] =
        "SELECT pg_class.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE pg_class.relname='{}' and nspname='{}'";

    /** select name, position, is_nullable, default, type, and is primary key for each column */
    static constexpr char SCHEMA_QUERY[] =
        "SELECT column_name, ordinal_position, is_nullable::boolean, "
        "       column_default, atttypid, "
        "       coalesce((pga.attnum=any(pgi.indkey))::boolean, false) as is_pkey "
        "FROM pg_catalog.pg_attribute pga "
        "JOIN information_schema.columns "
        "ON column_name=pga.attname "
        "LEFT OUTER JOIN pg_catalog.pg_index pgi "
        "ON pga.attrelid=pgi.indrelid AND pgi.indisprimary "
        "WHERE pga.attrelid={} "
        "AND table_schema='{}' "
        "AND table_name='{}' "
        "ORDER BY ordinal_position";

    static constexpr char SECONDARY_INDEX_QUERY[] =
        "SELECT"
        "    i.relname AS index_name, "
        "    a.attname AS column_name, "
        "    s.snum AS secondary_index_num, "
        "    a.attnum AS column_attnum "
        "FROM pg_index idx "
        "JOIN pg_class i ON i.oid = idx.indexrelid "
        "JOIN pg_attribute a ON a.attrelid = idx.indrelid "
        "JOIN generate_subscripts(idx.indkey, 1) s(snum) ON idx.indkey[s.snum] = a.attnum "
        "LEFT JOIN pg_constraint c ON c.conindid = idx.indexrelid AND c.contype = 'p' "
        "WHERE idx.indrelid = {} "
        "  AND c.conname IS NULL "
        "ORDER BY i.relname, index_name, s.snum ";

    /** select current xmin:xmax:list of xids in progress from DB as start of this transaction */
    static constexpr char XID_QUERY[] = "SELECT txid_current_snapshot()";

    /** copy command, output in binary using utf-8 encoding */
    static constexpr char COPY_QUERY[] = "COPY \"{}\".\"{}\" TO STDOUT WITH (FORMAT binary, ENCODING 'UTF-8')";


    /**
     * @brief Connect to database
     *
     * @param hostname DB hostname
     * @param username DB username
     * @param password DB password
     * @param port     DB port
     * @throws PgQueryError
     */
    void PgCopyTable::connect(const std::string &hostname,
                              const std::string &username,
                              const std::string &password,
                              const int port)
    {
        if (_connection.is_connected()) {
            throw PgAlreadyConnectedError();
        }

        _connection.connect(hostname, _db_name, username, password, port, false);

        try {
            _connection.start_transaction();
        } catch (PgQueryError &e) {
            _connection.disconnect();
            SPDLOG_ERROR("Error starting transaction failed");
            throw e;
        }
    }

    /**
     * @brief Disconnect connection, ignore errors
     */
    void PgCopyTable::disconnect()
    {
        _connection.disconnect();
    }


    void PgCopyTable::_get_xact_xids()
    {
        _connection.exec(XID_QUERY);
        if (_connection.ntuples() == 0) {
            _connection.clear();
            SPDLOG_ERROR("Unexpected results from query: {}", XID_QUERY);
            throw PgQueryError();
        }

        _schema.xids = _connection.get_string(0, 0);

        _connection.clear();
    }

    void PgCopyTable::_get_table_oid()
    {
        // escape the name strings
        std::unique_ptr<char[]> table_name = _connection.escape_string(_table_name);
        std::unique_ptr<char[]> schema_name = _connection.escape_string(_schema_name);

        _connection.exec(fmt::format(TABLE_OID_QUERY, table_name.get(), schema_name.get()));
        if (_connection.ntuples() == 0) {
            _connection.clear();
            SPDLOG_ERROR("Unexpected results from query: {}", TABLE_OID_QUERY);
            throw PgQueryError();
        }

        _schema.table_oid = _connection.get_int32(0, 0);

        _connection.clear();
    }

    void PgCopyTable::_get_secondary_indexes()
    {
        _connection.exec(fmt::format(SECONDARY_INDEX_QUERY, _schema.table_oid));
        if (_connection.ntuples() == 0) {
            _connection.clear();
            return;  // there are no secondary indexes
        }

        // iterate through results and generate vector of secondary indexes
        std::map<std::string, std::vector<std::string>> secondary_indexes;
        for (int i = 0; i < _connection.ntuples(); i++) {
            std::string index_name = _connection.get_string(i, 0);
            std::string column_name = _connection.get_string(i, 1);

            // add column name to map indexed by index_name
            // columns are ordered in query
            secondary_indexes[index_name].push_back(column_name);
        }

        // go through map and generate secondary keys in PgCopyTable object
        for (auto &index : secondary_indexes) {
            _schema.secondary_keys.push_back(index.second);
        }

        _connection.clear();
    }


    void PgCopyTable::_get_schema()
    {
        std::unique_ptr<char[]> table_name = _connection.escape_string(_table_name);
        std::unique_ptr<char[]> schema_name = _connection.escape_string(_schema_name);

        _connection.exec(fmt::format(SCHEMA_QUERY, _schema.table_oid,
                                     schema_name.get(), table_name.get()));

        if (_connection.ntuples() == 0) {
            SPDLOG_ERROR("Table not found: {}.{}", _schema_name, _table_name);
            _connection.clear();
            throw PgQueryError();
        }

        if (_connection.nfields() != 6) {
            SPDLOG_ERROR("Error: unexpected data from schema query or table not found");
            SPDLOG_ERROR("fields: {}, tuples: {}", _connection.nfields(), _connection.ntuples());
            _connection.clear();
            throw PgQueryError();
        }

        try {
            _schema.db_name = _db_name;
            _schema.table_name = _table_name;
            _schema.schema_name = _schema_name;

            // get columns
            int rows = _connection.ntuples();
            _schema.columns.resize(rows);

            for (int i = 0; i < rows; i++) {
                // add column to schema
                PgColumn column;

                // column_name string
                column.name = _connection.get_string(i, 0);

                // ordinal position int4
                column.position = _connection.get_int32(i, 1);

                // is_nullable varchar
                column.is_nullable = _connection.get_boolean(i, 2);

                // column_default varchar
                column.default_value = _connection.get_string_optional(i, 3);

                // atttypid oid
                column.pg_type = _connection.get_int32(i, 4);

                // is primary key
                column.is_pkey = _connection.get_boolean(i, 5);

                SPDLOG_DEBUG_MODULE(LOG_PG_REPL,
                                    "Column: {} type={} position={} nullable={} default_value={} pkey={}",
                                    column.name, column.pg_type, column.position, column.is_nullable,
                                    column.default_value.value_or("NULL"), column.is_pkey);

                _schema.columns[i] = column;

                // add the key to the list of pkeys
                if (column.is_pkey) {
                    _schema.pkeys.push_back(column.name);
                }
            }
        } catch (...) {
            _connection.clear();

            std::exception_ptr eptr;
            if (eptr) {
                std::rethrow_exception(eptr);
            }
        }

        _connection.clear();

    }

    void
    PgCopyTable::_prepare_copy()
    {
        std::unique_ptr<char[]> table_name = _connection.escape_string(_table_name);
        std::unique_ptr<char[]> schema_name = _connection.escape_string(_schema_name);

        _connection.exec(fmt::format(COPY_QUERY, schema_name.get(), table_name.get()));

        if (_connection.status() != PGRES_COPY_OUT) {
            SPDLOG_ERROR("Copy command did not receive PGRES_COPY_OUT");
            _connection.clear();
            throw PgQueryError();
        }

        // some sanity checks
        if (_connection.binary_tuples() != 1) {
            SPDLOG_ERROR("Copy command not outputting binary");
            _connection.clear();
            throw PgQueryError();
        }

        if (static_cast<std::size_t>(_connection.nfields()) != _schema.columns.size()) {
            SPDLOG_ERROR("Mismatch in copy fields");
            _connection.clear();
            throw PgQueryError();
        }

        _connection.clear();
    }

    std::optional<std::string_view>
    PgCopyTable::_get_next_data()
    {
        char *buffer = nullptr;
        while (true) {
            int r = _connection.get_copy_data(false);
            buffer = _connection.get_copy_buffer();
            if (r == -1) {
                // end of copy, get final result
                if (_connection.status() != PGRES_COMMAND_OK) {
                    SPDLOG_ERROR("Finished copy, got not-ok status: {}",
                                 static_cast<int>(_connection.status()));
                    _connection.clear();
                    throw PgQueryError();
                }
                _connection.clear();

                return std::nullopt; // no return value means we are at the end of the COPY
            } else if (r == -2) {
                // an error occured
                SPDLOG_ERROR("Copy command error: {}", _connection.error_message());
                throw PgQueryError();
            } else if (r == 0 || buffer == nullptr) {
                continue;
            }

            return std::string_view(buffer, r);
        }
    }

    void
    PgCopyTable::_release_data()
    {
        // make sure that the connection's copy buffer is cleared
        _connection.free_copy_buffer();
    }

    std::vector<PgMsgSchemaColumn>
    PgCopyTable::_map_to_pg_msg(const std::vector<PgColumn> pg_columns,
                                const std::vector<std::string> pkeys)
    {
        std::vector<PgMsgSchemaColumn> columns;
        columns.reserve(pg_columns.size());
        for(const PgColumn &pg_col : pg_columns){
            columns.push_back(PgMsgSchemaColumn{
                    pg_col.name,
                    static_cast<uint8_t>(_convert_pg_type(pg_col.pg_type)),
                    pg_col.pg_type,
                    pg_col.default_value,
                    pg_col.position,
                    pg_col.is_pkey ? _get_vec_pos(pkeys, pg_col.name) : -1, // pk_position
                    pg_col.is_nullable,
                    pg_col.is_pkey,
                    // TODO: we assume false since we don't support generated fields right now
                    false  // is_generated
                });

            SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "PKEY? {} {}", columns.back().is_pkey, columns.back().pk_position);
        }
        return columns;
    }

    int
    PgCopyTable::_get_vec_pos(const std::vector<std::string> vec,
                              const std::string element)
    {
        auto it = std::find(vec.begin(), vec.end(), element);
        if (it == vec.end()) {
            return -1;
        } else {
            return std::distance(vec.begin(), it);
        }
    }

    int32_t
    PgCopyTable::copy_to_springtail(const std::filesystem::path &base_dir,
                                    uint64_t xid)
    {
        _get_table_oid();
        _get_xact_xids();
        _get_schema();
        _get_secondary_indexes();

        // TODO: put start_xid and end_xid somewhere from _schema.xids

        // create the table metadata
        PgMsgTable create_msg{0, // pg lsn
                              static_cast<uint32_t>(_schema.table_oid),
                              0, // pg xid
                              _schema.schema_name,
                              _schema.table_name,
                              _map_to_pg_msg(_schema.columns, _schema.pkeys)};

        // note: we create the system metadata at the previous XID
        // XXX need to fix this
        uint64_t access_xid = xid - 1;
        TableMgr::get_instance()->create_table(access_xid, 0, create_msg);

        auto schema = SchemaMgr::get_instance()->get_extent_schema(_schema.table_oid, XidLsn(access_xid));
        auto table = TableMgr::get_instance()->get_mutable_table(_schema.table_oid, access_xid, xid);

        // start the COPY
        _prepare_copy();

        // get a chunk of data
        auto data = _get_next_data();
        if (!data) {
            throw PgIOError();
        }

        // verify the header before processing the rows
        int32_t ext_length = _verify_copy_header(data->substr(0, 19));
        size_t pos = 19 + ext_length;

        // scan the rows and populate the table
        while (true) {
            if (data->size() == pos) {
                // release the row data
                _release_data();

                // try to get more row data
                pos = 0;
                data = _get_next_data();
                if (!data) {
                    break; // finished with the COPY
                }
                continue; // got more data, keep processing
            }

            auto fields = _parse_row(*data, pos);
            if (!fields) {
                break; // saw footer, finished with the COPY
            }

            // construct a tuple from the row
            auto tuple = std::make_shared<FieldTuple>(fields, nullptr);

            // add the row to the table
            table->insert(tuple, xid, constant::UNKNOWN_EXTENT);
        }

        auto roots = table->finalize();

        // store the roots into the system table
        TableMgr::get_instance()->update_roots(_schema.table_oid, access_xid, xid, roots);

        return _schema.table_oid;
    }

    int32_t PgCopyTable::_verify_copy_header(const std::string_view &header)
    {
        // verify signature
        int r = std::memcmp(header.data(), COPY_SIGNATURE, 11);
        if (r != 0) {
            SPDLOG_ERROR("Signature doesn't match");
            throw PgUnknownMessageError();
        }

        int32_t flags = recvint32(header.data() + 11);
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "header flags: 0x{:X}", flags);
        if ((flags >> 16) & 0x1) {
            // bit 16 tells us if oids are present
            _oid_flag = true;
        }

        // return the length of any header extension
        return recvint32(header.data() + 15);
    }

    FieldArrayPtr
    PgCopyTable::_parse_row(const std::string_view &row, size_t &pos)
    {
        // start with 16 bit integer -- number of fields
        int16_t num_columns = recvint16(row.data() + pos);
        if (num_columns == -1) {
            // this is the footer
            return nullptr;
        }
        pos += 2;

        if (_oid_flag) {
            pos += 4; // skip the OID
        }

        auto fields = std::make_shared<FieldArray>();

        // iterate through columns
        for (int i = 0; i < num_columns; i++) {
            int32_t length = recvint32(row.data() + pos);
            pos += 4;

            // get the underlying springtail type
            int32_t pg_type = _schema.columns[i].pg_type;
            auto type = _convert_pg_type(pg_type);
            SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Got type {} from {}", static_cast<uint8_t>(type), pg_type);

            // check if null
            if (length == -1) {
                fields->push_back(std::make_shared<ConstNullField>(type));
                continue;
            }

            // otherwise store the data accordingly
            switch (type) {
            case (SchemaType::TEXT):
                fields->push_back(std::make_shared<ConstTypeField<std::string>>(std::string(row.data() + pos, length)));
                pos += length;
                break;

            case (SchemaType::INT64):
                assert(length == 8);
                fields->push_back(std::make_shared<ConstTypeField<int64_t>>(recvint64(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT32):
                assert(length == 4);
                fields->push_back(std::make_shared<ConstTypeField<int32_t>>(recvint32(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT16):
                assert(length == 2);
                fields->push_back(std::make_shared<ConstTypeField<int16_t>>(recvint16(row.data() + pos)));
                pos += length;
                break;

            case (SchemaType::INT8):
                assert(length == 1);
                fields->push_back(std::make_shared<ConstTypeField<int8_t>>(recvint8(row.data() + pos)));
                ++pos;
                break;

            case (SchemaType::BOOLEAN):
                assert(length == 1);
                fields->push_back(std::make_shared<ConstTypeField<bool>>(*(row.data() + pos) == 1));
                ++pos;
                break;

            case (SchemaType::FLOAT64): {
                assert(length == 8);
                auto num = recvint64(row.data() + pos);
                double d = *reinterpret_cast<double *>(&num);
                fields->push_back(std::make_shared<ConstTypeField<double>>(d));
                pos += length;
                break;
            }

            case (SchemaType::FLOAT32): {
                assert(length == 4);
                auto num = recvint32(row.data() + pos);
                float f = *reinterpret_cast<float *>(&num);
                fields->push_back(std::make_shared<ConstTypeField<float>>(f));
                pos += length;
                break;
            }

            case (SchemaType::BINARY): {
                std::string_view tmp(row.data() + pos, length);

                SPDLOG_WARN("Converting unsupported type '{}' into BINARY -- {}",
                            pg_type, tmp);
                // XXX print out the binary data here
                std::vector<char> data(tmp.begin(), tmp.end());
                fields->push_back(std::make_shared<ConstTypeField<std::vector<char>>>(data));
                pos += length;
                break;
            }

            default:
                throw TypeError(fmt::format("PG doesn't support type: {}",
                                            static_cast<uint8_t>(type)));
            }
        }

        return fields;
    }

    SchemaType
    PgCopyTable::_convert_pg_type(int32_t pg_type)
    {
        switch (pg_type) {
        case INT4OID:
        case DATEOID:
            return SchemaType::INT32;

        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            return SchemaType::TEXT;

        case INT8OID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case TIMEOID:
        case TIMETZOID:
        case MONEYOID:
            return SchemaType::INT64;

        case BOOLOID:
            return SchemaType::BOOLEAN;

        case INT2OID:
            return SchemaType::INT16;

        case FLOAT4OID:
            return SchemaType::FLOAT32;

        case FLOAT8OID:
            return SchemaType::FLOAT64;

        case CHAROID:
            return SchemaType::INT8;

        default:
            // put all other types into BINARY data for now
            return SchemaType::BINARY;
        }
    }
}
