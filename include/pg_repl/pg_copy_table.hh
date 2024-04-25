#pragma once

#include <cstdio>
#include <string>
#include <memory>

#include <pg_repl/libpq_connection.hh>

namespace springtail
{

    /** Stores the column schema for the table being copied */
    struct PgColumn {
        std::string name;
        std::string type;
        std::optional<std::string> default_value;
        int32_t position;
        bool is_nullable;
        bool is_pkey;
    };

    /** Stores the table schema for table being copied */
    struct PgTableSchema {
        std::string db_name;
        std::string schema_name;
        std::string table_name;
        std::string xids;                // txid_current_snapshot(); xmin:xmax:xids
        int32_t table_oid;
        std::vector<PgColumn> columns;
        std::vector<std::string> pkeys;  // primary keys as columns
    };


    /**
     * @brief Serialize data for a single table from a remote
     * Postgres server to a file, and de-serialize from a file
     */
    class PgCopyTable {

    private:
        /** magic number for header of the file */
        static inline constexpr uint32_t HEADER_MAGIC = 0x43219876;

        /** header of copy data signature */
        static inline constexpr char COPY_SIGNATURE[] = "PGCOPY\n\377\r\n\0";

        LibPqConnection _connection;
        std::string _db_name;
        std::string _schema_name;
        std::string _table_name;
        std::string _filename;

        bool _oid_flag = false;

        PgTableSchema _schema;

        std::FILE *_file = nullptr;

        // helper functions
        // write to file
        void write_int32(const int32_t val);
        void write_string(const std::string &str);
        void write_string(const char *str, unsigned len);
        void write_string(const std::optional<std::string> str);
        void write_bool(const bool val);

        // read from file
        int64_t read_int64();
        int32_t read_int32();
        int16_t read_int16();
        char read_char();
        bool read_bool();
        std::optional<std::string> read_string_optional();
        std::string read_string();
        std::string read_string(int length);

        // retrieve schema, write out schema and copy data
        void get_schema();
        void get_xact_xids();
        void get_table_oid();
        void get_pkeys();
        void write_schema();
        void copy_data();

        // read in schema, copy header, copy data
        void verify_copy_header();
        void read_schema();
        void read_copy_data();

    public:

        /**
         * @brief Constructor for copying table from remote system and writing to file
         *
         * @param db_name database name
         * @param schema_name schema name
         * @param table_name table name
         * @param filename output filename
         */
        PgCopyTable(const std::string &db_name,
                    const std::string &schema_name,
                    const std::string &table_name,
                    const std::string filename) :
            _db_name(db_name),
            _schema_name(schema_name),
            _table_name(table_name),
            _filename(filename)
        {}

        /**
         * @brief Empty constructor for reading data back from file
         * @param filename input filename
         */
        PgCopyTable(const std::string filename) : _filename(filename)
        {}

        ~PgCopyTable()
        {
            // release underlying connection if connected
            _connection.disconnect();
        }

        /**
         * @brief Connect to database; call prior to copyToFile
         *
         * @param hostname DB hostname
         * @param username DB username
         * @param password DB password
         * @param snapshot Snapshot ID
         * @param port     DB port
         */
        void connect(const std::string &hostname,
                     const std::string &username,
                     const std::string &password,
                     const int port);

        /**
         * @brief Disconnect connection; should be done after copy is finished
         */
        void disconnect();

        /**
         * @brief Copy remote table data to file
         *        add schema of table to header
         *
         * @param filename name of file to write data to
         */
        void copy_to_file();

        /**
         * @brief Decode data written to file by copyToFile()
         *
         * @param filename name of file to read data from
         */
        void decode_file();
    };
}
