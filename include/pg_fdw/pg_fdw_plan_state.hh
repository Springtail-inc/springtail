#pragma once

#include <common/constants.hh>

extern "C" {
    #include <postgres.h>
    #include <nodes/pg_list.h>
    #include <nodes/value.h>
}


namespace springtail::pg_fdw {

    /** Type to hold List* pointers */
    struct PgList {
        PgList() : _l{NIL}
        {}
        explicit PgList(List* l) : _l{l} {}

        size_t size() const {
            return list_length(_l);
        }

        List* _l;
    };


    /** Vector-style representation of List */
    template<typename T, bool is_integral=std::is_integral_v<T>> struct PgVector;

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
            ListCell *lc = nullptr;
            foreach(lc, _l) {
                if (i == pos) {
                    lfirst(lc) = v._l;
                }
                ++pos;
            }
        }
    };

    /** Serializable internal state to be passed between FDW callbacks
     * as fdw_private. The state is a list of List's with the following structure:
     *
     * root list
     * (
     *      (1 192464 24) - (db_id, tid, xid)
     *      (1 4) - (relation_rows relation_width)
     *      ("a" "b") - target column names (could be empty)
     *      (1, 2) - corresponding target column attributes (could be empty)
     *      (0 1) - this is to indicate quals that are to be ignored =0.
     *              For example self-referencing quals or unsupported functions.
     *      (42) - sort index if any
     *      (1) - scan direction 1-DESC, 0-ASC.
     * )
     *
     * note: Even so SpringtailPlanState is wrapper around a pointer to List*, it doesn't manage
     * the lifespan of the memory pointed by List*. It is managed by the PG executor.
     */
    class SpringtailPlanState {
    public:
        struct TableRef {
            uint64_t db_id;
            uint64_t tid;
            uint64_t xid;
        };

        struct ColumnInfo {
            std::string name;
            int16_t attrno;
        };

        SpringtailPlanState() = default;
        SpringtailPlanState( uint64_t db_id, uint64_t tid, uint64_t xid);
        explicit SpringtailPlanState(List* s);

        SpringtailPlanState(const SpringtailPlanState&) = delete;
        SpringtailPlanState& operator=(const SpringtailPlanState&) = delete;

        /** Get the table ids. */
        TableRef get_table_ref() const;

        List* fdw_private() const {
            return _state._l;
        }

        // methods to access the TARGET_COL_x lists
        void add_target_column(const std::string& name, int16_t attrno);
        size_t count_target_columns() const;
        ColumnInfo get_target_column(size_t i) const;

        // methods to access with the QUAL_STATE list
        void set_qual_state(size_t i, bool ignore);
        bool get_qual_state(size_t i) const;

        // methods to access the SORT_INDEX list
        void set_sort_index(uint64_t id);
        std::optional<uint64_t> get_sort_index() const;

        // methods to access the SCAN_DIRECTION list
        void set_scan_direction(bool scan_asc);
        bool is_scan_asc() const;

        // methods to access the REL_SIZE list
        void set_rel_size(uint64_t rows, uint64_t width);
        uint64_t get_rel_rows() const;
        uint64_t get_rel_width() const;

        // methods to access cached planning metadata (indexes matching quals/joins)
        void set_cached_qual_indexes(const std::vector<uint64_t>& indexes);
        PgVector<uint64_t> get_cached_qual_indexes() const;

        void set_cached_join_indexes(const std::vector<uint64_t>& indexes);
        PgVector<uint64_t> get_cached_join_indexes() const;

        /** Indexes of various List's in the root list of the state */
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
            // cached planning metadata
            CACHED_QUAL_INDEXES,
            CACHED_JOIN_INDEXES,

            LAST
        };

    private:
        // indexes in TABLE_REF
        enum TableIdIndex {
            DB_ID,
            TID,
            XID
        };
        // indexes in REL_SIZE
        enum RelSizeIndex {
            ROWS,
            WIDTH
        };

        // the root list
        PgVector<List*> _state;
    };


    // this is to map List indexes to the corresponding value types
    template<int ListIndex>
    struct ListValueType {
        using type = uint64_t;
    };
    template<> 
    struct ListValueType<SpringtailPlanState::RootIndex::TARGET_COL_NAME>
    {
        using type = std::string;
    };
    template<>
    struct ListValueType<SpringtailPlanState::RootIndex::TARGET_COL_ATTR>
    {
        // type used by PG for attribute numbers
        using type = int16_t;
    };
}

