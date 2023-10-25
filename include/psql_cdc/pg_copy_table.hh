#pragma once

#include <cstdio>
#include <string>

#include <libpq-fe.h>

namespace springtail
{

    struct PgColumn {
        std::string name;
        std::string type;
        std::string default_value;
        int32_t position;
        bool is_nullable;
        bool is_pkey;
    };

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

        PGconn *_connection = nullptr;
        std::string _db_name;
        std::string _schema_name;
        std::string _table_name;
        std::string _filename;

        bool _oid_flag = false;

        PgTableSchema _schema;

        std::FILE *_file = nullptr;

        // helper functions
        // write to file
        void writeInt32(const int32_t val);
        void writeString(const std::string &str);
        void writeString(const char *str);
        void writeBool(const bool val);

        // read from file
        int64_t readInt64();
        int32_t readInt32();
        int16_t readInt16();
        char readChar();
        bool readBool();
        std::string readString();
        std::string readString(int length);

        // retrieve schema, write out schema and copy data
        void getSchema();
        void getXactXids();
        void getTableOid();
        void getPkeys();
        void writeSchema();
        void copyData();

        // read in schema, copy header, copy data
        void verifyCopyHeader();
        void readSchema();
        void readCopyData();

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
            if (_connection != nullptr) {
                PQfinish(_connection);
            }
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
        void copyToFile();

        /**
         * @brief Decode data written to file by copyToFile()
         *
         * @param filename name of file to read data from
         */
        void decodeFile();
    };
}