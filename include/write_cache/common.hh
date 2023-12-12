#pragma once

#include <string>
#include <string_view>

namespace springtail {
    namespace WriteCache {
        /**
         * @brief Table operation type
         */
        enum TableOp : char {
            TRUNCATE='T',
            SCHEMA_CHANGE='S'
        };

        /**
         * @brief Row operation type
         */
        enum RowOp : char {
            INSERT='I',
            UPDATE='U',
            DELETE='D'
        };

        /**
         * @brief Data returned when a row is fetched
         */
        struct RowData {
            uint64_t XID;
            uint64_t LSN;         
            uint64_t RID;   
            RowOp op;
            const std::string_view pkey;
            const std::string_view data;

            /**
             * @brief Construct a new Row Data object
             */
            RowData(RowOp op, uint64_t LSN, uint64_t RID, uint64_t XID, const char * const pkey,
                     int pkey_len, const char * const data, int data_len) :
                XID(XID), LSN(LSN), RID(RID), op(op), pkey(pkey, pkey_len), data(data, data_len)
            { }

            RowData() {}
        };
        
        /**
         * @brief Data returned when table changes are fetched
         */
        struct TableChange {
            uint64_t XID;
            uint64_t LSN;
            TableOp  op;

            /**
             * @brief Construct a new Table Change object
             */
            TableChange(TableOp op, uint64_t LSN, uint64_t XID) : XID(XID), LSN(LSN), op(op)
            { }

            TableChange() {}
        };    
     }
}