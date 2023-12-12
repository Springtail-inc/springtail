#include <common/properties.hh>
#include <common/logging.hh>
#include <common/redis.hh>

#include <iostream>
#include <cassert>

#include <write_cache/write_cache.hh>

namespace {

    /**
     * @brief Add item to index if either item does not exist or item's score > score passed in
     * Updates the extent index, then the table index, then the global table index
     */
    const char *redis_update_idxs_xmin_lua = R"(
        local eid_key = KEYS[1]
        local tid_key = KEYS[2]
        local tbl_idx_key = KEYS[3]

        local rid = ARGV[1]
        local eid = ARGV[2]
        local tid = ARGV[3]
        local score = ARGV[4]

        local function add_to_index(key, item)
            local existing_score = redis.call('ZSCORE', key, item)
            if existing_score == nil or tonumber(existing_score) > tonumber(score) then
                redis.call('ZADD', key, score, item)
            end        
        end

        add_to_index(eid_key, rid)
        add_to_index(tid_key, eid)
        add_to_index(tbl_idx_key, tid)

        return 1)";

    /**
     * @brief Lookup all RIDs from the extent index <= given XID; store into temp sorted set
     *        Remove row data from RID index for each RID returned <= XID
     *        Remove temporary sorted set
     *        Remove all RIDs from extent index <= XID
     */
    const char *redis_remove_rows_by_xid_lua = R"(
        local eid_key = KEYS[1]
        local tmp_rid_key = KEYS[2]

        local score = ARGV[1]

        local function delete_rids(rids)
            for _, rid in ipairs(rids)
                local rid_key = eid_key .. ":" .. rid
                redis.call('ZREMRANGEBYSCORE', rid_key, 0, score)
            end
        end

        redis.call('ZRANGESTORE', tmp_rid_key, eid_key, 0, score, 'BYSCORE')
        local cursor = 0
        repeat
            local results = redis.call('ZSCAN', tmp_rid_key, cursor, 'COUNT', 100)
            cursor = results[1]
            delete_rids(results[2])
        until cursor == 0

        redis.call('DEL', tmp_rid_key)
        redis.call('ZREMRANGEBYSCORE', eid_key, 0, score)
    )";
}

namespace springtail {
    /* static initialization must happen outside of class */
    WriteCache* WriteCache::_instance {nullptr};
    std::mutex WriteCache::_instance_mutex;

    /** 
     * Write cache Redis Indexes
     * CHG:TID: [ TableChange @ XID ] -- sorted set, list of table changes sort by XID, 1 entry per XID
     * TBLIDX: [ TID @ Xmin ] -- sorted set, list of table IDs (global table idx) sorted by min XID, 1 entry per TID
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

        _add_sorted_set_by_xid(_get_rid_index(tid, eid, rid), row_data, xid);

        // need to fixup higher level indexes to mark them as dirty at this xid
        _mark_indexes_dirty(tid, eid, rid, xid);
    }

    void 
    WriteCache::update_row(uint64_t tid, uint64_t old_eid, uint64_t new_eid,
                           uint64_t xid, uint64_t LSN,
                           const std::string_view &old_pkey, 
                           const std::string_view &new_pkey, 
                           const std::string_view &data)
    {
        uint64_t old_rid = _get_rid(old_pkey);
        uint64_t new_rid = _get_rid(new_pkey);

        // if row pkeys and eid are the same then just do the insert/update
        if (old_rid == new_rid && old_eid == new_eid) {
            std::string row_data = _serialize_row(new_pkey, data, LSN, RowOp::UPDATE);
            _add_sorted_set_by_xid(_get_rid_index(tid, new_eid, new_rid), data, xid);
            _mark_indexes_dirty(tid, new_eid, new_rid, xid);        
            return;
        }
        // if old pkey and new pkey are different or old eid and new eid are different
        // do a delete then an insert
        std::string row_data = _serialize_row(old_pkey, {}, LSN, RowOp::DELETE);
        _add_sorted_set_by_xid(_get_rid_index(tid, old_eid, old_rid), row_data, xid);        

        row_data = _serialize_row(new_pkey, data, LSN, RowOp::INSERT);
        _add_sorted_set_by_xid(_get_rid_index(tid, new_eid, new_rid), row_data, xid);

        // need to fixup higher level indexes to mark them as dirty at this xid
        _mark_indexes_dirty(tid, old_eid, old_rid, xid);                
        _mark_indexes_dirty(tid, new_eid, new_rid, xid);        
    }
    
    void
    WriteCache::delete_row(uint64_t tid, uint64_t eid, 
                           uint64_t xid, uint64_t LSN,
                           const std::string_view &pkey)
    {
        uint64_t rid = _get_rid(pkey);
        std::string row_data = _serialize_row(pkey, {}, LSN, RowOp::DELETE);
        _add_sorted_set_by_xid(_get_rid_index(tid, eid, rid), row_data, xid);

        // need to fixup higher level indexes to mark them as dirty at this xid
        _mark_indexes_dirty(tid, eid, rid, xid);        
    }

    void 
    WriteCache::_mark_indexes_dirty(uint64_t tid, uint64_t eid, uint64_t rid, uint64_t xid)
    {
        // mark EID index dirty if RID doesn't exist, or XID is less than existing XID for RID
        // mark TID index dirty if EID doesn't exist, or XID is less than existing XID for EID
        // mark Table index dirty if TID doesn't exist or XID is less than existing XID for TID
        _update_sorted_sets(_get_eid_index(tid, eid), _get_tid_index(tid), _get_table_index(),
                            std::to_string(rid), std::to_string(eid), std::to_string(tid), std::to_string(xid));
    }

    void 
    WriteCache::clean_extent(uint64_t tid, uint64_t eid, uint64_t xid)
    {
        // find all RIDs within extent index <= XID -> store these in a new set
        // scan the new set, issuing a delete for all RID indexes -- batch these deletes
        // remove RIDs from extent index
        _remove_rows_from_extent(_get_eid_index(tid, eid), std::to_string(xid));

        // find min XID from extent index, update or remove EID entry in TID index
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
        std::vector<std::pair<std::string, double>> items;
        items = _get_sorted_set_by_xid(key, xid, count, offset);

        std::vector<uint64_t> ids(items.size());
        for (int i = 0; i < items.size(); i++) {
            ids[i] = (std::stoll(items[i].first));
        }

        return ids;
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
    WriteCache::_update_sorted_sets(const std::string &eid_key,
                                    const std::string &tid_key,
                                    const std::string &tbl_idx_key,
                                    const std::string &rid_item,
                                    const std::string &eid_item,
                                    const std::string &tid_item,
                                    const std::string &xid_score)
    {
        std::shared_ptr<RedisClient> redis = RedisMgr::get_instance()->get_client();
        
        // script takes: 3 keys: eid idx, tid idx, global tid idx
        //               3 value args: rid, eid, tid
        //               1 score arg: xid
        redis->eval<long long>(redis_update_idxs_xmin_lua, {eid_key, tid_key, tbl_idx_key}, 
                               {rid_item, eid_item, tid_item, xid_score});
    }

    void 
    WriteCache::_remove_rows_from_extent(const std::string &eid_key, const std::string &xid_score)
    {
        std::string tmp_rid_key = fmt::format("tmp:{}:xid:{}", eid_key, xid_score);
        std::shared_ptr<RedisClient> redis = RedisMgr::get_instance()->get_client();

        redis->eval<long long>(redis_remove_rows_by_xid_lua, {eid_key, tmp_rid_key}, {xid_score});

        auto cursor = 0LL;

        std::vector<std::pair<std::string,double>> result;

        auto pipe = redis->pipeline(false);
        auto replies = pipe.command('ZRANGESTORE', tmp_rid_key, eid_key, 0, xid_score, "BYSCORE")
            .command('ZSCAN', tmp_rid_key, cursor, "COUNT", 100, std::back_inserter(result))
            .exec();

        replies.get(1);

        int pipe_cmds = 0;
        for (auto &p: result) {
            pipe.(p.first);
            pipe_cmds++;

            if (pipe_cmds >= 10) {
                pipe.exec();
                pipe_cmds = 0;
            }
        }
/*
                     redis.call('ZREMRANGEBYSCORE', rid_key, 0, score)
            end
        end

        redis.call('ZRANGESTORE', tmp_rid_key, eid_key, 0, score, 'BYSCORE')
        local cursor = 0
        repeat
            local results = redis.call('ZSCAN', tmp_rid_key, cursor, 'COUNT', 100)
            cursor = results[1]
            delete_rids(results[2])
        until cursor == 0

        redis.call('DEL', tmp_rid_key)
        redis.call('ZREMRANGEBYSCORE', eid_key, 0, score)
        */
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
        if (op != RowOp::DELETE) {
            data_len = data.length();
            s.insert(pos, reinterpret_cast<char *>(&data_len), 4);
            pos += 4;
            s.insert(pos, data);
        }

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

        // get data and length; if delete then set ptr to pkey so string_view works
        uint32_t data_len = 0;
        const char *data_start = pkey_start;
        if (op != RowOp::DELETE) {
            std::copy_n(data.c_str() + pos, 4, reinterpret_cast<char *>(&data_len));
            pos += 4;
            data_start = data.c_str() + pos;
        }

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


