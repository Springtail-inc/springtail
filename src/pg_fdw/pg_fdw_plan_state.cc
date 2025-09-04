#include <common/logging.hh>
#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_fdw_plan_state.hh>

extern "C" {
    #include <nodes/makefuncs.h>
}

using namespace springtail::pg_fdw;

FdwPlanState::FdwPlanState(uint64_t db_id, uint64_t tid, uint64_t xid)
{
    // we build a fixed structure of the plan state
    // * {db_id, tid, xid} - identifiers
    // * [{target_column_name, attr}] - target columns
    // table info
    {
        PgVector<uint64_t> v;
        v.push_back(db_id);
        v.push_back(tid);
        v.push_back(xid);

        _state.push_back(v);
    }


    // rel size
    {
        // column names
        PgVector<uint64_t> v;
        _state.push_back(v);
    }

    // target list
    {
        // column names
        PgVector<std::string> v;
        _state.push_back(v);
    }

    {
        // column attributes
        PgVector<int16_t> v;
        _state.push_back(v);
    }

    // qual states
    {
        // LHS attrno
        PgVector<int16_t> v;
        _state.push_back(v);
    }

    // sort index id
    {
        PgVector<uint64_t> v;
        _state.push_back(v);
    }

    // planstate
    {
        PgVector<std::string> v;
        _state.push_back(v);
    }
}

FdwPlanState::FdwPlanState(List* s) : _state{s}
{
    DCHECK(_state._l);

    PgVector<std::string> t{_state[RootIndex::PLANSTATE]};
    _planstate = t[0];

    DCHECK(!_planstate.empty());
}

FdwPlanState::TableRef FdwPlanState::get_table_ref() const
{
    PgVector<uint64_t> t{_state[RootIndex::TABLE_REF]};
    return TableRef{t[TableIdIndex::DB_ID],
        t[TableIdIndex::TID],
        t[TableIdIndex::XID]};

}

size_t FdwPlanState::count_target_columns() 
{
    PgVector<std::string> names{_state[RootIndex::TARGET_COL_NAME]};
    return names.size();
}

FdwPlanState::ColumnInfo FdwPlanState::get_target_column(size_t i) 
{
    PgVector<std::string> names{_state[RootIndex::TARGET_COL_NAME]};
    PgVector<int16_t> attr{_state[RootIndex::TARGET_COL_ATTR]};
    return {names[i], attr[i]};
}

void FdwPlanState::add_target_colum(const std::string& name, int16_t attrno) 
{
    PgVector<std::string> names{_state[RootIndex::TARGET_COL_NAME]};
    PgVector<int16_t> attr{_state[RootIndex::TARGET_COL_ATTR]};

    DCHECK(names.size() == attr.size());

    names.push_back(name);
    attr.push_back(attrno);

    // update the state pointers
    _state.replace(RootIndex::TARGET_COL_NAME, names);
    _state.replace(RootIndex::TARGET_COL_ATTR, attr);
}

void FdwPlanState::set_qual_state(size_t i, bool ignore)
{
    PgVector<int16_t> v{_state[RootIndex::QUAL_STATE]};
    DCHECK(i == v.size()); //must be adding to the end
    v.push_back(ignore?0:1);
    _state.replace(RootIndex::QUAL_STATE, v);
}

bool FdwPlanState::get_qual_state(size_t i)
{
    PgVector<int16_t> v{_state[RootIndex::QUAL_STATE]};
    return v[i];
}

void FdwPlanState::set_sort_index(uint64_t id)
{
    PgVector<int16_t> v{_state[RootIndex::SORT_INDEX]};
    v._l = NIL;
    v.push_back(id);
    _state.replace(RootIndex::SORT_INDEX, v);
}
void FdwPlanState::remove_sort_index()
{
    PgVector<int16_t> v{_state[RootIndex::SORT_INDEX]};
    v._l = NIL;
    _state.replace(RootIndex::SORT_INDEX, v);
}
std::optional<uint64_t> 
FdwPlanState::get_sort_index()
{
    PgVector<int16_t> v{_state[RootIndex::SORT_INDEX]};
    if (!v.size()) {
        return {};
    }
    return v[0];
}

void FdwPlanState::set_planstate(void* p) {
    PgVector<std::string> v;
    std::ostringstream ss;
    ss << (uint64_t)p;
    auto s = ss.str();
    v.push_back(ss.str());
    _state.replace(RootIndex::PLANSTATE, v);
}

void FdwPlanState::set_rel_size(uint64_t rows, uint64_t width) 
{
    PgVector<uint64_t> v{_state[RootIndex::REL_SIZE]};
    v.push_back(rows);
    v.push_back(width);
    _state.replace(RootIndex::REL_SIZE, v);
}

uint64_t FdwPlanState::get_rel_rows() 
{
    PgVector<uint64_t> v{_state[RootIndex::REL_SIZE]};
    return v[RelSizeIndex::ROWS];
}

uint64_t FdwPlanState::get_rel_width()
{
    PgVector<uint64_t> v{_state[RootIndex::REL_SIZE]};
    return v[RelSizeIndex::WIDTH];
}
