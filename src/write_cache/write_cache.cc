#include <common/properties.hh>
#include <common/logging.hh>
#include <common/redis.hh>

#include <cassert>

#include <write_cache/write_cache.hh>

namespace springtail {
    /* static initialization must happen outside of class */
    WriteCache* WriteCache::_instance {nullptr};
    std::mutex WriteCache::_instance_mutex;

    /** 
     * Write cache Redis Indexes
     * CHG:TID: [ TableChange @ XID ] -- sorted set, list of table changes sort by XID, 1 entry per XID
     * TBLIDX: [ TID @ Xmin ] -- sorted set, list of table IDs sorted by min XID, 1 entry per TID
     * TID: [ EID @ Xmin ] -- sorted set, list of extent IDs sorted by min XID, 1 entry per EID
     * EID: [ RID @ Xmin ] -- sorted set, list of row IDs sorted by min XID, 1 entry per RID
     * RID: [ RowData @ XID ] -- sorted set, list of row data sorted by XID, 1 entry per XID
     */

    WriteCache *
    WriteCache::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new WriteCache();
        }

        return _instance;
    }

    void
    WriteCache::shutdown()
    {
         std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    void 
    WriteCache::start_gc(uint64_t xid)
    {
    }
    
    void 
    WriteCache::complete_gc(uint64_t xid)
    {
    }

    void 
    WriteCache::insert_table_change(uint64_t tid, uint64_t xid, uint64_t LSN, TableOp op)
    {
        std::string data = _serialize_table_change(LSN, op);
        _add_sorted_set_by_xid(_get_table_change_index(tid), data, xid);
    }

    void 
    WriteCache::insert_row(uint64_t tid, uint64_t eid, 
                           uint64_t xid, uint64_t LSN,
                           const std::string_view &pkey, 
                           const std::string_view &data)
    {
        uint64_t rid = _get_rid(pkey);

        std::string row_data = _serialize_row(pkey, data, LSN, RowOp::INSERT);

        _add_sorted_set_by_xid(_get_rid_index(tid, eid, rid), data, xid);

        // need to fixup higher level indexes to mark them as dirty at this xid
    }

    void 
    WriteCache::update_row(uint64_t tid, uint64_t old_eid, uint64_t new_eid,
                           uint64_t xid, uint64_t LSN,
                           const std::string_view &old_pkey, 
                           const std::string_view &new_pkey, 
                           const std::string_view &data)
    {
    }
    
    void
    WriteCache::delete_row(uint64_t tid, uint64_t eid, 
                           uint64_t xid, uint64_t LSN,
                           std::string_view &pkey)
    {
    }

    void 
    WriteCache::clean_extent(uint64_t tid, uint64_t eid, uint64_t xid)
    {
    }

    void 
    WriteCache::evict(uint64_t xid)
    {
    }

    std::vector<std::shared_ptr<WriteCache::TableChange>> 
    WriteCache::fetch_table_changes(uint64_t tid, uint64_t xid)
    {
        std::vector<std::pair<std::string, double>> rows;
        rows = _get_sorted_set_by_xid(_get_table_change_index(tid), xid);
        
        std::vector<std::shared_ptr<WriteCache::TableChange>> result(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            result[i] = _deserialize_table_change(rows[i].first, rows[i].second);
        }

        return result;
    }
    
    std::vector<uint64_t>
    WriteCache::fetch_tables(uint64_t xid, int count, uint64_t offset)
    {
        return _fetch_ids(_get_table_index(), xid, count, offset);
    }

    std::vector<uint64_t>
    WriteCache::fetch_extents(uint64_t tid, uint64_t xid, int count, uint64_t offset)
    {
        return _fetch_ids(_get_tid_index(tid), xid, count, offset);        
    }
    
    std::vector<uint64_t>
    WriteCache::fetch_rows(uint64_t tid, uint64_t eid, uint64_t xid, int count, uint64_t offset)
    {
        return _fetch_ids(_get_eid_index(tid, eid), xid, count, offset);        
    }

    std::shared_ptr<WriteCache::RowData>
    WriteCache::fetch_row(uint64_t tid, uint64_t eid, uint64_t rid, uint64_t xid)
    {
        std::vector<std::pair<std::string, double>> rows;
        rows = _get_sorted_set_by_xid(_get_rid_index(tid, eid, rid), xid, 1);
        if (rows.size() == 0) {
            return nullptr;
        }
        assert(rows.size() == 1);

        return _deserialize_row(rows[0].first, xid, rid);
    }

    std::vector<uint64_t>
    WriteCache::_fetch_ids(const std::string &key, uint64_t xid, int count, uint64_t offset)
    {
        std::vector<std::pair<std::string, double>> rows;
        rows = _get_sorted_set_by_xid(key, xid, count, offset);

        std::vector<uint64_t> rids(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            rids[i] = (std::stoll(rows[i].first));
        }

        return rids;
    }
    
    std::vector<std::pair<std::string, double>> 
    WriteCache::_get_sorted_set_by_xid(const std::string &key, uint64_t xid, int count, uint64_t offset)
    {
        std::vector<std::pair<std::string, double>> result;

        sw::redis::LimitOptions limits;
        limits.offset = offset;
        limits.count = count;
        
        std::shared_ptr<RedisClient> redis = RedisMgr::get_instance()->get_client();
        redis->zrangebyscore(key, sw::redis::RightBoundedInterval<double>(xid, sw::redis::BoundType::CLOSED),
                             limits, std::back_inserter(result));

        return result;
    }

    void 
    WriteCache::_add_sorted_set_by_xid(const std::string &key, const std::string_view &data, uint64_t xid)
    {
        std::shared_ptr<RedisClient> redis = RedisMgr::get_instance()->get_client();
        redis->zadd(key, {std::make_pair(data, xid)});
    }

    void 
    WriteCache::_remove_sorted_set_by_xid(const std::string &key, uint64_t xid)
    {
        std::shared_ptr<RedisClient> redis = RedisMgr::get_instance()->get_client();
        redis->zremrangebyscore(key, sw::redis::RightBoundedInterval<double>(xid, sw::redis::BoundType::CLOSED));
    }

    std::string 
    WriteCache::_serialize_row(const std::string_view &pkey, const std::string_view &data, uint64_t LSN, RowOp op)
    {
        std::string s;
        s.resize(pkey.length() + data.length() + 8 + 8 + 1);

        // add op
        int pos = 0;
        s.insert(pos, 1, op);
        pos++;

        // add LSN
        s.insert(pos, reinterpret_cast<char *>(&LSN), 8);
        pos += 8;

        // add pkey
        uint32_t data_len;
        data_len = pkey.length();
        s.insert(pos, reinterpret_cast<char *>(&data_len), 4);
        pos += 4;
        s.insert(pos, pkey);
        pos += data_len;

        // add data
        data_len = data.length();
        s.insert(pos, reinterpret_cast<char *>(&data_len), 4);
        pos += 4;
        s.insert(pos, data);

        return s;
    }

    std::string 
    WriteCache::_serialize_table_change(uint64_t LSN, TableOp op)
    {
        std::string s;
        s.resize(8 + 1);

        s.insert(0, 1, op);
        s.insert(1, reinterpret_cast<char *>(&LSN), 4);
        
        return s;
    }

    std::shared_ptr<WriteCache::RowData>
    WriteCache::_deserialize_row(std::string &data, uint64_t XID, uint64_t RID)
    {
        int pos = 0;
        
        // copy row op 1B
        RowOp op = static_cast<RowOp>(data[pos]);
        pos++;

        // copy LSN 8B
        uint64_t LSN;
        std::copy_n(data.c_str() + pos, 8, reinterpret_cast<char *>(&LSN));
        pos += 8;

        // get pkey and length
        uint32_t pkey_len;
        std::copy_n(data.c_str() + pos, 4, reinterpret_cast<char *>(&pkey_len));
        pos += 4;
        const char *pkey_start = data.c_str() + pos;
        pos += pkey_len;

        // get data and length
        uint32_t data_len;
        std::copy_n(data.c_str() + pos, 4, reinterpret_cast<char *>(&data_len));
        pos += 4;
        const char *data_start = data.c_str() + pos;

        return std::make_shared<RowData>(op, LSN, XID, RID, pkey_start, pkey_len, data_start, data_len, std::move(data));
    }

    std::shared_ptr<WriteCache::TableChange>
    WriteCache::_deserialize_table_change(const std::string &data, uint64_t XID)
    {
        TableOp op = static_cast<TableOp>(data[0]);
        uint64_t LSN;
        std::copy_n(data.c_str() + 1, 8, reinterpret_cast<char *>(&LSN));
        
        return std::make_shared<TableChange>(op, LSN, XID);
    }

    uint64_t
    WriteCache::_get_rid(const std::string_view &pkey) 
    {
        uint64_t rid=0;
        return rid;
    }
}


