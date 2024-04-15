#include "storage/field.hh"
#include <cassert>
#include <cstdint>
#include <optional>
#include <sstream>
#include <vector>

#include <fmt/core.h>

// springtail includes
#include <common/common.hh>
#include <pg_repl/exception.hh>
#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_stream_table.hh>
#include <pg_repl/libpq_connection.hh>

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
        "       column_default, udt_name, "
        "       coalesce((pga.attnum=any(pgi.indkey))::boolean, false) as is_pkey "
        "FROM pg_catalog.pg_attribute pga "
        "JOIN information_schema.columns "
        "ON column_name=pga.attname "
        "LEFT OUTER JOIN pg_catalog.pg_index pgi "
        "ON pga.attrelid=pgi.indrelid "
        "WHERE pga.attrelid={} "
        "AND table_schema='{}' "
        "AND table_name='{}' "
        "ORDER BY ordinal_position";

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
    void PgStreamTable::connect(const std::string &hostname,
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
            std::cerr << "Error starting transaction failed\n";
            throw e;
        }
    }


    /**
     * @brief Disconnect connection, ignore errors
     */
    void PgStreamTable::disconnect()
    {
        _connection.disconnect();
    }


    /**
     * @brief Get transaction ids for current transaction snapshot
     * @details calls: SELECT txid_current_snapshot() returns string
     *          in format: "xid_min:xid_max:xid,xid,xid" showing set of
     *          transactions overlapping with current transaction
     *
     * @throws PgQueryError
     */
    std::string PgStreamTable::get_xact_xids()
    {
        _connection.exec(XID_QUERY);
        if (_connection.ntuples() == 0) {
            _connection.clear();
            std::cerr << "Unexpected results from query: " << XID_QUERY << std::endl;
            throw PgQueryError();
        }

        std::string xids = _connection.get_string(0, 0);

        _connection.clear();
        return xids;
    }


    /**
     * @brief Get table's oid based on schema / table, store in schema
     */
    void PgStreamTable::get_table_oid()
    {
        _connection.exec(fmt::format(TABLE_OID_QUERY, _table_name, _schema_name));
        if (_connection.ntuples() == 0) {
            _connection.clear();
            std::cerr << "Unexpected results from query: " << TABLE_OID_QUERY << std::endl;
            throw PgQueryError();
        }

        _schema.table_oid = _connection.get_int32(0, 0);

        _connection.clear();
    }


    /**
     * @brief Extract schema from table and store in internal _schema object
     * @details Uses udt_name from information_catalog.columns table for name of type
     *          Saves the column name, ordinal position, default value (as string), column type
     *          and is_nullable flag for each table column.  Requires getTableOid() first.
     *
     */
    PgTableSchema PgStreamTable::get_schema()
    {
        std::unique_ptr<char[]> table_name = _connection.escape_string(_table_name);
        std::unique_ptr<char[]> schema_name = _connection.escape_string(_schema_name);

        _connection.exec(fmt::format(SCHEMA_QUERY, _schema.table_oid,
                                     schema_name.get(), table_name.get()));

        if (_connection.ntuples() == 0) {
            std::cerr << fmt::format("Table not found: {}.{}\n", _schema_name, _table_name);
            _connection.clear();
            throw PgQueryError();
        }

        if (_connection.nfields() != 6) {
            std::cerr << "Error: unexpected data from schema query or table not found\n";
            std::cerr << "fields: " << _connection.nfields() << ", tuples: "
                      << _connection.ntuples() << std::endl;
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

                // udt_name varchar
                column.type = _connection.get_string(i, 4);

                // is primary key
                column.is_pkey = _connection.get_boolean(i, 5);

                std::cout << fmt::format("Column: {} type={} position={} nullable={} default_value={} pkey={}\n",
                                         column.name, column.type, column.position, column.is_nullable,
                                         column.default_value.value_or("NULL"), column.is_pkey);

                _schema.columns[i] = column;
            }
        } catch (...) {
            _connection.clear();

            std::exception_ptr eptr;
            if (eptr) {
                std::rethrow_exception(eptr);
            }
        }

        _connection.clear();

        return _schema;
    }


    /**
     * @brief Initiate copy command from server; dump data to file
     *
     */
    void PgStreamTable::copy_data()
    {
        std::unique_ptr<char[]> table_name = _connection.escape_string(_table_name);
        std::unique_ptr<char[]> schema_name = _connection.escape_string(_schema_name);

        _connection.exec(fmt::format(COPY_QUERY, schema_name.get(), table_name.get()));

        if (_connection.status() != PGRES_COPY_OUT) {
            std::cerr << "Copy command did not receive PGRES_COPY_OUT\n";
            _connection.clear();
            throw PgQueryError();
        }

        // some sanity checks
        if (_connection.binary_tuples() != 1) {
            std::cerr << "Copy command not outputting binary\n";
            _connection.clear();
            throw PgQueryError();
        }

        if (_connection.nfields() != _schema.columns.size()) {
            std::cerr << "Mismatch in copy fields\n";
            _connection.clear();
            throw PgQueryError();
        }

        _connection.clear();

        _buffer = nullptr;
    }

    std::optional<FieldArrayPtr> PgStreamTable::next_row() {
        std::optional<FieldArrayPtr> result;
        while (true) {
            // these two lines put a full row of data into the buffer
            int r = _connection.get_copy_data(false);
            _buffer = _connection.get_copy_buffer();
            if (r == -1) {
                // end of copy, get final result
                if (_connection.status() != PGRES_COMMAND_OK) {
                    std::cerr << "Finished copy, got not-ok status: " << _connection.status() << std::endl;
                    _connection.clear();
                    throw PgQueryError();
                }
                _connection.clear();
                // break;
                return {};
            } else if (r == -2) {
                // an error occured
                std::cerr << "Copy command error: " << _connection.error_message() << std::endl;
                throw PgQueryError();
            } else if (r == 0 || _buffer == nullptr) {
                continue;
            }

            // got a non-zero result, r indicates number of bytes
            // (shouldn't be null but check anyway)
            std::cout << fmt::format("Copy got: {} bytes\n", r);
            result = parse_row();

            _connection.free_copy_buffer();
            _buffer = nullptr;
        }
        return result;
    }

    std::optional<FieldArrayPtr> PgStreamTable::parse_row() {
        std::stringstream row_stream;
        row_stream << _buffer;
        // start with 16 bit integer -- number of fields
        int16_t num_columns = read_int16(row_stream);
        if (num_columns == -1) {
            // this is the footer
            return {};
        }
        auto fields = std::make_shared<FieldArray>(num_columns);

        if (_oid_flag) {
            int32_t oid = read_int32(row_stream);
            std::cout << "oid: " << oid << std::endl;
        }

        // iterate through columns
        for (int i = 0; i < num_columns; i++) {
            int32_t length = read_int32(row_stream);

            std::cout << fmt::format(" - column {} type={}, length={}, data=",
                                     _schema.columns[i].name,
                                     _schema.columns[i].type, length);

            std::string type = _schema.columns[i].type;

            // NOTE: a length of -1 indicates a NULL value
            if (length == -1) {
                // TODO implement this type
                // fields->at(i) = std::make_shared<ConstTypeField<NULL>>(NULL);
            }

            // a length of 0 indicates empty string
            if (length == 0 && (type == "text" || type == "varchar")) {
                fields->at(i) = std::make_shared<ConstTypeField<std::string>>("\n");
            }

            if (length > 0) {
                if (type == "int4" || type == "int" || type == "serial4" || type == "serial") {
                    assert(length == 4);
                    fields->at(i) = std::make_shared<ConstTypeField<int32_t>>(read_int32(row_stream));
                }
                else if (type == "text" || type == "varchar") {
                    fields->at(i) = std::make_shared<ConstTypeField<std::string>>(read_string(row_stream, length));
                }
                else if (type == "int8" || type == "serial8") {
                    assert(length == 8);
                    fields->at(i) = std::make_shared<ConstTypeField<int64_t>>(read_int64(row_stream));
                }
                else if (type == "bool") {
                    assert(length == 1);
                    fields->at(i) = std::make_shared<ConstTypeField<bool>>(read_bool(row_stream));
                }
                else if (type == "bpchar") {
                    // fixed length blank padded char
                    if (length == 1) {
                        char c = read_char(row_stream);
                        fields->at(i) = std::make_shared<ConstTypeField<char>>(c);
                    } else {
                        row_stream.seekg(length, std::ios_base::cur);
                    }
                }
                else if (type == "int2" || type == "serial2") {
                    assert(length == 2);
                    fields->at(i) = std::make_shared<ConstTypeField<int16_t>>(read_int16(row_stream));
                }
                else if (type == "float4") {
                    assert(length == 4);
                    int32_t num = read_int32(row_stream);
                    float f = *reinterpret_cast<float *>(&num);
                    fields->at(i) = std::make_shared<ConstTypeField<float>>(f);
                }
                else if (type == "float8") {
                    assert(length == 8);
                    int64_t num = read_int64(row_stream);
                    double d = *reinterpret_cast<double *>(&num);
                    fields->at(i) = std::make_shared<ConstTypeField<double>>(d);
                }
                else if (type == "timestamp" || type == "timestamptz") {
                    // micro seconds since 2000-01-01 00:00:00
                    assert(length == 8);
                    uint64_t ts = read_int64(row_stream);
                    uint64_t epoch_ms = ts/1000 + MSEC_SINCE_Y2K;
                    std::cout << ts << " : " << epoch_ms << std::endl;
                    // TODO implement this type
                    // fields->at(i) = std::make_shared<ConstTypeField<TIME>>(ts);
                }
                else if (type == "time") {
                    // micro seconds since day start
                    assert(length == 8);
                    uint64_t ts = read_int64(row_stream);
                    std::cout << ts << " : " << (ts/1000/1000/60/60) << " hrs\n";
                    // TODO implement this type
                    // fields->at(i) = std::make_shared<ConstTypeField<TIME>>(ts);
                }
                else if (type == "date") {
                    // days since 2000-01-01 00:00:00
                    assert(length == 4);
                    uint32_t dt = read_int32(row_stream);
                    uint64_t epoch_ms = dt * 24 * 60 * 60 * 1000L + MSEC_SINCE_Y2K;
                    std::cout << dt << " : " << epoch_ms << std::endl;
                    // TODO implement this type
                    // fields->at(i) = std::make_shared<ConstTypeField<TIME>>(dt);
                }
                else if (type == "_bpchar") {
                    // array of blank padded chars
                    row_stream.seekg(length, std::ios_base::cur);
                } else {
                    row_stream.seekg(length, std::ios_base::cur);
                }
                // TODO handle binary?
            }  // if (length > 0)
        }
        return fields;
    }


    /**
     * @brief Read boolean from file; copy stores 1 as true, 0 as false
     *
     * @return boolean
     */
    bool PgStreamTable::read_bool(std::stringstream &stream)
    {
        char c;
        stream.get(c);
        if (c) {
            return (c == 1);
        } else {
            throw PgIOError();
        }

    }


    /**
     * @brief Read 64 bit int (8B) from file
     * @return 64 bit int
     */
    int64_t PgStreamTable::read_int64(std::stringstream &stream)
    {
        char buffer[8];
        if (stream.read(buffer, 8)) {
            return recvint64(buffer);
        } else {
            throw PgIOError();
        }
    }


    /**
     * @brief Read 32 bit int (4B) from file
     * @return 32 bit int
     */
    int32_t PgStreamTable::read_int32(std::stringstream &stream)
    {
        char buffer[4];
        if (stream.read(buffer, 4)) {
            return recvint32(buffer);
        } else {
            throw PgIOError();
        }
    }


    /**
     * @brief Read 16 bit int (2B) from file
     * @return 16 bit int
     */
    int16_t PgStreamTable::read_int16(std::stringstream &stream)
    {
        char buffer[2];
        if (stream.read(buffer, 2)) {
            return recvint16(buffer);
        } else {
            throw PgIOError();
        }
    }


    char PgStreamTable::read_char(std::stringstream &stream)
    {
        char c;
        stream.get(c);
        if (c) {
            return c;
        } else {
            throw PgIOError();
        }
    }


    /**
     * @brief Read string from file; length followed by bytes; maps NULL to empty string
     *
     * @return string; NULL mapped to empty string
     */
    std::string PgStreamTable::read_string(std::stringstream &stream)
    {
        int len = read_int32(stream);
        if (len <= 0) {
            return "";
        }

        return read_string(stream, len);
    }


    /**
     * @brief Read string from file; length followed by bytes; maintains NULL
     *
     * @return optional string; optional is false if string is NULL
     */
    std::optional<std::string> PgStreamTable::read_string_optional(std::stringstream &stream)
    {
        int len = read_int32(stream);
        if (len == 0) {
            return "";
        }
        if (len == -1) {
            return {};
        }
        return read_string(stream, len);
    }

    /**
     * @brief Read string from file given length
     *
     * @param len length of string excluding null
     * @return string
     */
    std::string PgStreamTable::read_string(std::stringstream &stream, int len)
    {
        char *strbuf = new char[len + 1];
        if (stream.read(strbuf, len)) {
            strbuf[len] = '\0';

            std::string str(strbuf);
            free (strbuf);

            return str;
        } else {
            free(strbuf);
            throw PgIOError();
        }
    }
}
