#include <common/logging.hh>
#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_fdw_plan_state.hh>

extern "C" {
    #include <nodes/makefuncs.h>
}

using namespace springtail::pg_fdw;


SpringtailPlanState::SpringtailPlanState(uint64_t db_id, uint64_t tid, uint64_t xid)
{
    // init state List's
    for (int i = 0; i != RootIndex::LAST; ++i) {
        if (i == RootIndex::TARGET_COL_NAME) {
            PgVector<ListValueType<RootIndex::TARGET_COL_NAME>::type> v;
            _state.push_back(v);
        } 
        else if (i == RootIndex::TARGET_COL_ATTR) {
            PgVector<ListValueType<RootIndex::TARGET_COL_ATTR>::type> v;
            _state.push_back(v);
        } else {
            PgVector<uint64_t> v;
            _state.push_back(v);
        }
    }

    // set table ids
    {
        constexpr auto ind = RootIndex::TABLE_REF;
        PgVector<ListValueType<ind>::type> t{_state[ind]};

        t.push_back(db_id);
        t.push_back(tid);
        t.push_back(xid);
        _state.replace(ind, t);
    }

    CHECK(_state.size() == RootIndex::LAST);
}

SpringtailPlanState::SpringtailPlanState(List* s) : _state{s}
{
    DCHECK(_state._l);
}

SpringtailPlanState::TableRef
SpringtailPlanState::get_table_ref() const
{
    constexpr auto ind = RootIndex::TABLE_REF;

    PgVector<ListValueType<ind>::type> t{_state[ind]};

    return TableRef{t[TableIdIndex::DB_ID],
        t[TableIdIndex::TID],
        t[TableIdIndex::XID]};

}

size_t 
SpringtailPlanState::count_target_columns() const
{
    constexpr auto ind = RootIndex::TARGET_COL_NAME;
    PgVector<ListValueType<ind>::type> names{_state[ind]};
    return names.size();
}

SpringtailPlanState::ColumnInfo
SpringtailPlanState::get_target_column(size_t i) const
{
    PgVector<ListValueType<RootIndex::TARGET_COL_NAME>::type> names{_state[RootIndex::TARGET_COL_NAME]};
    PgVector<ListValueType<RootIndex::TARGET_COL_ATTR>::type> attr{_state[RootIndex::TARGET_COL_ATTR]};
    return {names[i], attr[i]};
}

void
SpringtailPlanState::add_target_column(const std::string& name, int16_t attrno) 
{
    PgVector<ListValueType<RootIndex::TARGET_COL_NAME>::type> names{_state[RootIndex::TARGET_COL_NAME]};
    PgVector<ListValueType<RootIndex::TARGET_COL_ATTR>::type> attr{_state[RootIndex::TARGET_COL_ATTR]};

    DCHECK(names.size() == attr.size());

    names.push_back(name);
    attr.push_back(attrno);

    // update the state pointers
    _state.replace(RootIndex::TARGET_COL_NAME, names);
    _state.replace(RootIndex::TARGET_COL_ATTR, attr);
}

void
SpringtailPlanState::set_qual_state(size_t i, bool ignore)
{
    constexpr auto ind = RootIndex::QUAL_STATE;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    DCHECK(i == v.size()); //must be adding to the end
    v.push_back(ignore?0:1);
    _state.replace(ind, v);
}

bool
SpringtailPlanState::get_qual_state(size_t i) const
{
    constexpr auto ind = RootIndex::QUAL_STATE;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    return v[i];
}

void
SpringtailPlanState::set_sort_index(uint64_t id)
{
    constexpr auto ind = RootIndex::SORT_INDEX;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    v._l = NIL;
    v.push_back(id);
    _state.replace(ind, v);
}

std::optional<uint64_t> 
SpringtailPlanState::get_sort_index() const
{
    constexpr auto ind = RootIndex::SORT_INDEX;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    if (!v.size()) {
        return {};
    }
    return v[0];
}
void
SpringtailPlanState::set_scan_direction(bool scan_asc)
{
    constexpr auto ind = RootIndex::SCAN_DIRECTION;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    v._l = NIL;
    v.push_back(scan_asc?1:0);
    _state.replace(ind, v);
}
bool 
SpringtailPlanState::is_scan_asc() const
{
    constexpr auto ind = RootIndex::SCAN_DIRECTION;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    if (!v.size()) {
        return true;
    }
    return v[0]?true:false;
}

void SpringtailPlanState::set_rel_size(uint64_t rows, uint64_t width) 
{
    constexpr auto ind = RootIndex::REL_SIZE;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    v.push_back(rows);
    v.push_back(width);
    _state.replace(ind, v);
}

uint64_t SpringtailPlanState::get_rel_rows() const
{
    constexpr auto ind = RootIndex::REL_SIZE;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    return v[RelSizeIndex::ROWS];
}

uint64_t SpringtailPlanState::get_rel_width() const
{
    constexpr auto ind = RootIndex::REL_SIZE;
    PgVector<ListValueType<ind>::type> v{_state[ind]};
    return v[RelSizeIndex::WIDTH];
}
