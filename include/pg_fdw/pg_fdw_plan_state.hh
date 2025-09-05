#pragma once

#include <common/constants.hh>

extern "C" {
    #include <postgres.h>
    #include <nodes/pg_list.h>
    #include <nodes/value.h>
}


namespace springtail::pg_fdw {

    // C++ helpers for List*

    struct PgList {
        PgList() 
        {
            _l = NIL;
        }
        explicit PgList(List* l) : _l{l} {}

        size_t size() const {
            return list_length(_l);
        }

        List* _l;
    };

    template<typename T, bool is_integral=std::is_integral<T>::value> struct PgVector;

    // vector of integrals
    template <typename T>
    struct PgVector<T, true> : PgList 
    {
        using PgList::PgList;

        T operator[](size_t i) const {
            return intVal(list_nth(_l, i));
        }

        void push_back(T v) {
            _l = lappend(_l, makeInteger(v));
        }
    };

    // vector of strings
    template <>
    struct PgVector<std::string, false> : PgList 
    {
        using PgList::PgList;

        std::string operator[](size_t i) const {
            return strVal(list_nth(_l, i));
        }

        void push_back(const std::string& v) {
            _l = lappend(_l, makeString(pstrdup((char*)v.c_str())));
        }
    };

    // vector of vectors
    template <>
    struct PgVector<List*, false> : PgList 
    {
        using PgList::PgList;

        List* operator[](size_t i) const {
            return (List*)list_nth(_l, i);
        }

        void push_back(List* v) {
            _l = lappend(_l, v);
        }

        template<typename T>
        void push_back(const PgVector<T>& v) {
            _l = lappend(_l, v._l);
        }

        template<typename T>
        void replace(size_t i, const PgVector<T>& v) {
            DCHECK(i < size());
            size_t pos = 0; 
            ListCell *lc;
            foreach(lc, _l) {
                if (i == pos) {
                    lfirst(lc) = v._l;
                }
                ++pos;
            }
        }
    };

    /** Serializable internal state to be passed between FDW callbacks
     * as fdw_private. 
     */
    struct FdwPlanState {
        struct TableRef {
            uint64_t db_id;
            uint64_t tid;
            uint64_t xid;
        };

        struct ColumnInfo {
            std::string name;
            int16_t attrno;
        };

        std::string _planstate;

        FdwPlanState() = default;

        FdwPlanState( uint64_t db_id, uint64_t tid, uint64_t xid);
        explicit FdwPlanState(List* s);

        FdwPlanState(const FdwPlanState&) = delete;
        FdwPlanState& operator=(const FdwPlanState&) = delete;

        TableRef get_table_ref() const;


        List* fdw_private() const {
            return _state._l;
        }

        void add_target_colum(const std::string& name, int16_t attrno);
        size_t count_target_columns();
        ColumnInfo get_target_column(size_t i);

        void set_qual_state(size_t i, bool ignore);
        bool get_qual_state(size_t i);

        void set_sort_index(uint64_t id);
        std::optional<uint64_t> get_sort_index();

        void set_scan_direction(bool scan_asc);
        bool is_scan_asc();

        void set_rel_size(uint64_t rows, uint64_t width);
        uint64_t get_rel_rows();
        uint64_t get_rel_width();
        
        void set_planstate(void*);

    private:
        enum RootIndex {
            TABLE_REF,
            REL_SIZE,
            TARGET_COL_NAME, //target colum name
            TARGET_COL_ATTR,
            //list of qual states 0-ignored, 1-active
            QUAL_STATE, 
            // sort index
            SORT_INDEX,
            // scan direction
            SCAN_DIRECTION,
            PLANSTATE
        };

        enum TableIdIndex {
            DB_ID,
            TID,
            XID
        };

        enum RelSizeIndex {
            ROWS,
            WIDTH
        };

        PgVector<List*> _state;
    };
}

