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

/* See: https://www.postgresql.org/docs/current/datatype.html for postgres types */

namespace springtail
{
    /** get oid for table */
    static const char *TABLE_OID_QUERY =
        "SELECT pg_class.oid "
        "FROM pg_catalog.pg_class "
        "JOIN pg_catalog.pg_namespace "
        "ON relnamespace=pg_namespace.oid "
        "WHERE pg_class.relname='{}' and nspname='{}'";

    /** select name, position, is_nullable, default, type, and is primary key for each column */
    static const char *SCHEMA_QUERY =
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
    static const char *XID_QUERY = "SELECT txid_current_snapshot()";

    /** copy command, output in binary using utf-8 encoding */
    static const char *COPY_QUERY = "COPY \"{}\".\"{}\" TO STDOUT WITH (FORMAT binary, ENCODING 'UTF-8')";


    /**
     * @brief Connect to database
     *
     * @param hostname DB hostname
     * @param username DB username
     * @param password DB password
     * @param snapshot Snapshot ID
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
            std::cerr << "Error starting transaction failed\n";
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


    /**
     * @brief Get transaction ids for current transaction snapshot
     * @details calls: SELECT txid_current_snapshot() returns string
     *          in format: "xid_min:xid_max:xid,xid,xid" showing set of
     *          transactions overlapping with current transaction
     *
     * @throws PgQueryError
     */
    void PgCopyTable::get_xact_xids()
    {
        _connection.exec(XID_QUERY);
        if (_connection.ntuples() == 0) {
            _connection.clear();
            std::cerr << "Unexpected results from query: " << XID_QUERY << std::endl;
            throw PgQueryError();
        }

        _schema.xids = _connection.get_string(0, 0);

        _connection.clear();
    }


    /**
     * @brief Get table's oid based on schema / table, store in schema
     */
    void PgCopyTable::get_table_oid()
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
    void PgCopyTable::get_schema()
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
    }


    /**
     * @brief Read boolean from file; copy stores 1 as true, 0 as false
     *
     * @return boolean
     */
    bool PgCopyTable::read_bool()
    {
        int c = getc(_file);
        if (c == EOF) {
            throw PgIOError();
        }

        return (c == 1);
    }


    /**
     * @brief Read 64 bit int (8B) from file
     * @return 64 bit int
     */
    int64_t PgCopyTable::read_int64()
    {
        char buffer[8];
        int r = std::fread(buffer, 1, 8, _file);
        if (r != 8) {
            throw PgIOError();
        }
        return recvint64(buffer);
    }


    /**
     * @brief Read 32 bit int (4B) from file
     * @return 32 bit int
     */
    int32_t PgCopyTable::read_int32()
    {
        char buffer[4];
        int r = std::fread(buffer, 1, 4, _file);
        if (r != 4) {
            throw PgIOError();
        }
        return recvint32(buffer);
    }


    /**
     * @brief Read 16 bit int (2B) from file
     * @return 16 bit int
     */
    int16_t PgCopyTable::read_int16()
    {
        char buffer[2];
        int r = std::fread(buffer, 1, 2, _file);
        if (r != 2) {
            throw PgIOError();
        }
        return recvint16(buffer);
    }


    char PgCopyTable::read_char()
    {
        int c = getc(_file);
        if (c == EOF) {
            throw PgIOError();
        }

        return c;
    }


    /**
     * @brief Read string from file; length followed by bytes; maps NULL to empty string
     *
     * @return string; NULL mapped to empty string
     */
    std::string PgCopyTable::read_string()
    {
        int len = read_int32();
        if (len <= 0) {
            return "";
        }

        return read_string(len);
    }


    /**
     * @brief Read string from file; length followed by bytes; maintains NULL
     *
     * @return optional string; optional is false if string is NULL
     */
    std::optional<std::string> PgCopyTable::read_string_optional()
    {
        int len = read_int32();
        if (len == 0) {
            return "";
        }
        if (len == -1) {
            return {};
        }
        return read_string(len);
    }

    /**
     * @brief Read string from file given length
     *
     * @param len length of string excluding null
     * @return string
     */
    std::string PgCopyTable::read_string(int len)
    {
        char *strbuf = new char[len + 1];
        int r = std::fread(strbuf, 1, len, _file);
        if (r != len) {
            free(strbuf);
            throw PgIOError();
        }
        strbuf[len] = '\0';

        std::string str(strbuf);
        free (strbuf);

        return str;
    }

    /**
     * @brief Write out 32 bit int (4B)
     *
     * @param val value to write out
     */
    void PgCopyTable::write_int32(const int32_t val)
    {
        char buffer[4];
        sendint32(val, buffer);
        int r = std::fwrite(buffer, 1, 4, _file);
        if (r != 4) {
            std::cerr << "write_int32: wrote too few bytes\n";
            throw PgIOError();
        }
    }

    /**
     * @brief Helper to call write_string(const char *)
     *
     * @param str string to write out
     */
    void PgCopyTable::write_string(const std::string &str)
    {
        write_string(str.c_str(), str.length());
    }

    /**
     * @brief Helper to call write_string
     *
     * @param str string to write out
     */
    void PgCopyTable::write_string(const std::optional<std::string> str)
    {
        if (!str.has_value()) {
            // write null
            write_int32(-1);
            return;
        }

        write_string(str.value());
    }

    /**
     * @brief Write a string, length (excluding null) followed by bytes (excluding null)
     *
     * @param str String value to write out
     */
    void PgCopyTable::write_string(const char *str, unsigned len)
    {
        write_int32(len);
        if (len == 0) {
            return;
        }

        int r = std::fwrite(str, 1, len, _file);
        if (r != len) {
            std::cerr << fmt::format("write_string: wrote {} bytes instead of {} bytes\n", r, len);
            throw PgIOError();
        }
    }


    /**
     * @brief Write boolean as single character: 1 or 0
     *
     * @param val boolean value to write
     */
    void PgCopyTable::write_bool(const bool val) {
        int r = std::putc((val ? 1: 0), _file);
        if (r == EOF) {
            std::cerr << "Error writing bool, got EOF\n";
            throw PgIOError();
        }
    }


    /**
     * @brief Write schema to file
     * @details Format:
     *          Uint32 (4B) magic number
     *          String (4B length + varchar) database name
     *          String (4B length + varchar) schema name
     *          String (4B length + varchar) table name
     *          String (4B length + varchar) xids (xmin:xmax:xid,xid,...)
     *          Int32 (4B) table oid
     *          Int32 (4B) number of columns
     *          For each column:
     *          Int32 (4B) ordinal position
     *          Bool (1B) is nullable (1 or 0)
     *          Bool (1B) is primary key
     *          String (4B length + varchar) column name
     *          String (4B length + varchar) type name (e.g., int4)
     *          String (4B length + varchar) default value
     */
    void PgCopyTable::write_schema()
    {
        write_int32(HEADER_MAGIC);
        write_string(_schema.db_name);
        write_string(_schema.schema_name);
        write_string(_schema.table_name);
        write_string(_schema.xids);
        write_int32(_schema.table_oid);

        write_int32(_schema.columns.size());
        for (int i = 0; i < _schema.columns.size(); i++) {
            write_int32(_schema.columns[i].position);
            write_bool(_schema.columns[i].is_nullable);
            write_bool(_schema.columns[i].is_pkey);
            write_string(_schema.columns[i].name);
            write_string(_schema.columns[i].type);
            write_string(_schema.columns[i].default_value);
        }
    }


    /**
     * @brief Read schema header from file; populate internal _schema object
     */
    void PgCopyTable::read_schema()
    {
        int32_t r = read_int32();
        if (r != HEADER_MAGIC) {
            std::cerr << "ReadSchema failed, header magic not matching\n";
            throw PgIOError();
        }

        _schema.db_name = read_string();
        _schema.schema_name = read_string();
        _schema.table_name = read_string();
        _schema.xids = read_string();
        _schema.table_oid = read_int32();

        int cols = read_int32(); // columns
        _schema.columns.resize(cols);

        std::cout << fmt::format("Schema: {}.{}.{}\nXids: {}\n",
                                 _schema.db_name, _schema.schema_name, _schema.table_name,
                                 _schema.xids);

        for (int i = 0; i < cols; i++) {
            _schema.columns[i].position = read_int32();
            _schema.columns[i].is_nullable = read_bool();
            _schema.columns[i].is_pkey = read_bool();
            _schema.columns[i].name = read_string();
            _schema.columns[i].type = read_string();
            _schema.columns[i].default_value = read_string_optional();

            std::cout << fmt::format("Column: {} type={} position={} nullable={} default_value={} pkey={}\n",
                                     _schema.columns[i].name, _schema.columns[i].type,
                                     _schema.columns[i].position,
                                     _schema.columns[i].is_nullable,
                                     _schema.columns[i].default_value.value_or("NULL"),
                                     _schema.columns[i].is_pkey);
        }
    }


    /**
     * @brief Initiate copy command from server; dump data to file
     *
     */
    void PgCopyTable::copy_data()
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

        char *buffer = nullptr;
        while (true) {
            int r = _connection.get_copy_data(false);
            buffer = _connection.get_copy_buffer();
            if (r == -1) {
                // end of copy, get final result
                if (_connection.status() != PGRES_COMMAND_OK) {
                    std::cerr << "Finished copy, got not-ok status: " << _connection.status() << std::endl;
                    _connection.clear();
                    throw PgQueryError();
                }
                _connection.clear();
                break;
            } else if (r == -2) {
                // an error occured
                std::cerr << "Copy command error: " << _connection.error_message() << std::endl;
                throw PgQueryError();
            } else if (r == 0 || buffer == nullptr) {
                continue;
            }

            // got a non-zero result, r indicates number of bytes
            // (shouldn't be null but check anyway)
            std::cout << fmt::format("Copy got: {} bytes\n", r);
            if (std::fwrite(buffer, 1, r, _file) < r) {
                std::cerr << "Error writing copy data\n";
                _connection.free_copy_buffer();
                throw PgIOError();
            }

            _connection.free_copy_buffer();
            buffer = nullptr;
        }
    }


    /**
     * @brief Copy remote table data to file
     *        add schema of table to header
     *
     * @param filename name of file to write data to
     */
    void PgCopyTable::copy_to_file()
    {
        // open file
        _file = std::fopen(_filename.c_str(), "wb");

        get_table_oid();
        get_xact_xids();
        get_schema();
        write_schema();
        copy_data();

        std::fclose(_file);
        _file = nullptr;
    }


    /**
     * @brief Decode data written to file by copyToFile()
     *
     * @param filename name of file to read data from
     */
    void PgCopyTable::decode_file()
    {
        std::cout << "Opening file: " << _filename << std::endl;
        _file = std::fopen(_filename.c_str(), "rb");

        read_schema();
        verify_copy_header();
        read_copy_data();

        std::fclose(_file);
        _file = nullptr;
    }


    /**
     * @brief Validate copy header
     * @details Header contents:
     *          11B signature starts with COPY_SIGNATURE
     *           4B flags; bit 16 oid flag
     *           4B header extension length
     */
    void PgCopyTable::verify_copy_header()
    {
        char header[19];

        /*
         * 11B signature
         *  4B flags
         *  4B extension length
         */
        int r = std::fread(header, 1, 19, _file);
        if (r != 19) {
            std::cerr << "Couldn't read signature\n";
            throw PgIOError();
        }

        // verify signature
        r = std::memcmp(header, COPY_SIGNATURE, 11);
        if (r != 0) {
            std::cerr << "Signature doesn't match\n";
            throw PgUnknownMessageError();
        }

        int32_t flags = recvint32(&header[11]);
        std::cout << fmt::format("header flags: 0x{:X}\n", flags);
        if ((flags >> 16) & 0x1) {
            // bit 16 tells us if oids are present
            _oid_flag = true;
        }

        // skip any header extension
        int32_t ext_length = recvint32(&header[15]);
        if (ext_length > 0) {
            std::fseek(_file, ext_length, SEEK_CUR);
        }
    }


    /**
     * @brief Read copy data from file for a table
     */
    void PgCopyTable::read_copy_data()
    {
        while (true) {
            // start with 16 bit integer -- number of fields
            int16_t num_columns = read_int16();
            if (num_columns == -1) {
                // this is the footer
                break;
            }
            std::cout << "columns: " << num_columns << std::endl;

            if (_oid_flag) {
                int32_t oid = read_int32();
                std::cout << "oid: " << oid << std::endl;
            }

            // iterate through columns
            for (int i = 0; i < num_columns; i++) {
                int32_t length = read_int32();

                std::cout << fmt::format(" - column {} type={}, length={}, data=",
                                         _schema.columns[i].name,
                                         _schema.columns[i].type, length);

                std::string type = _schema.columns[i].type;

                // NOTE: a length of -1 indicates a NULL value
                if (length == -1) {
                    std::cout << "NULL\n";
                }

                // a length of 0 indicates empty string
                if (length == 0 && (type == "text" || type == "varchar")) {
                    std::cout << "\n"; // empty string
                }

                if (length > 0) {
                    if (type == "int4" || type == "int" || type == "serial4" || type == "serial") {
                        assert(length == 4);
                        std::cout << read_int32() << std::endl;
                    }
                    else if (type == "text" || type == "varchar") {
                        std::cout << read_string(length) << std::endl;
                    }
                    else if (type == "int8" || type == "serial8") {
                        assert(length == 8);
                        std::cout << read_int64() << std::endl;
                    }
                    else if (type == "bool") {
                        assert(length == 1);
                        std::cout << read_bool() << std::endl;
                    }
                    else if (type == "bpchar") {
                        // fixed length blank padded char
                        if (length == 1) {
                            char c = read_char();
                            std::cout << c << std::endl;
                        } else {
                            std::fseek(_file, length, SEEK_CUR);
                        }
                    }
                    else if (type == "int2" || type == "serial2") {
                        assert(length == 2);
                        std::cout << read_int16() << std::endl;
                    }
                    else if (type == "float4") {
                        assert(length == 4);
                        int32_t num = read_int32();
                        float f = *reinterpret_cast<float *>(&num);
                        std::cout << f << std::endl;
                    }
                    else if (type == "float8") {
                        assert(length == 8);
                        int64_t num = read_int64();
                        double d = *reinterpret_cast<double *>(&num);
                        std::cout << d << std::endl;
                    }
                    else if (type == "timestamp" || type == "timestamptz") {
                        // micro seconds since 2000-01-01 00:00:00
                        assert(length == 8);
                        uint64_t ts = read_int64();
                        uint64_t epoch_ms = ts/1000 + MSEC_SINCE_Y2K;
                        std::cout << ts << " : " << epoch_ms << std::endl;
                    }
                    else if (type == "time") {
                        // micro seconds since day start
                        assert(length == 8);
                        uint64_t ts = read_int64();
                        std::cout << ts << " : " << (ts/1000/1000/60/60) << " hrs\n";
                    }
                    else if (type == "date") {
                        // days since 2000-01-01 00:00:00
                        assert(length == 4);
                        uint32_t dt = read_int32();
                        uint64_t epoch_ms = dt * 24 * 60 * 60 * 1000L + MSEC_SINCE_Y2K;
                        std::cout << dt << " : " << epoch_ms << std::endl;
                    }
                    else if (type == "_bpchar") {
                        // array of blank padded chars
                        std::fseek(_file, length, SEEK_CUR);
                    } else {
                        std::fseek(_file, length, SEEK_CUR);
                    }
                }  // if (length > 0)
            }
        }
    }
}

#if 0
int main(int argc, char* argv[])
{
    try {
        if (argc < 3) {
            std::cerr << "Usage: " << argv[0] << " table_name filename\n";
            return -1;
        }
        springtail::springtail_init();

        // write file out with copy data
        springtail::PgCopyTable table_out("springtail", "public", argv[1], argv[2]);
        table_out.connect("localhost", "springtail", "springtail", 5432);
        table_out.copy_to_file();
        table_out.disconnect();

        // read file that was just written out
        springtail::PgCopyTable table_in(argv[2]);
        table_in.decode_file();

    } catch (springtail::Error &e) {
        std::cerr << "Caught error\n";
        e.print_trace();
        return -1;
    }

    return 0;
}
#endif

