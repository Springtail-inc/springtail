#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <utility>
#include <tuple>
#include <boost/functional/hash.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <absl/log/check.h>
#include <storage/xid.hh>

namespace springtail::sys_tbl_mgr {

    namespace bmi = boost::multi_index;

    using DbId = uint64_t;
    using ObjId = uint64_t; //< Object ID such as table ID
    using Xid = uint64_t;

    /** A cache for storing messages per (db, object_id, xid_lsn) key.
     * It uses LRU eviction policy.
     * The cache is thread-safe.
     * The cache doesn't own the containers used for caching.
     * The containers must be set using set_containers() before using the cache
     * and must be valid until the cache is destroyed.
     * The cache uses memory allocators provided by the caller.
     */
    template<typename Traits>
    struct MsgCache {
        // data types
        template<typename T>
        using Alloc = Traits::template Alloc<T>;

        using Key = std::pair<DbId, ObjId>;
        using Value = Traits::Value;
        using Mutex = Traits::Mutex;
        using Lock = Traits::Lock;
        using SharableLock = Traits::SharableLock;

        /** Message stored in the cache **/
        struct Message
        {
            explicit Message(const Value::allocator_type& al)
                :msg{al}
            {}
            XidLsn xid;
            Value msg;
            bool dropped = false;
        };
        using Cache = Traits::template Cache<Key, Message>;
        using Messages = Traits::template Messages<Message>;

        /** LRU key that is used for eviction and LRU lookup **/
        struct LruKey
        {
            DbId db;
            ObjId obj_id;
            XidLsn xid;
            bool operator==(const LruKey& rhs) const = default;
        };
        struct LruHashFunc
        {
            size_t operator()(const LruKey& k) const
            {
                using Tuple = std::tuple<DbId, ObjId, uint64_t, uint64_t>;
                Tuple t{k.db, k.obj_id, k.xid.xid, k.xid.lsn};
                return boost::hash<Tuple>{}(t);
            }
        };

        /** LRU is a multiple index container that keeps the insertion order (first index) 
         * and allows fast lookup by key (second index).
         */
        using Lru = bmi::multi_index_container<
            LruKey,
            bmi::indexed_by<
                bmi::sequenced<>, ///< this will keep the insertion order
                bmi::hashed_unique<bmi::identity<LruKey>, LruHashFunc> ///< no duplicates
            >, 
            Alloc<LruKey> >;

        /** Constructor.
         * @param _mutex The mutex to use for locking.
         * @param messages_alloc The allocator to use for the messages container.
         * @param value_alloc The allocator to use for the message value.
         */
        MsgCache(Mutex& mutex,
                Messages::allocator_type& messages_alloc,
                Value::allocator_type& value_alloc)
            :_mutex{mutex},
            _messages_alloc{messages_alloc},
            _value_alloc{value_alloc}
        {};

        MsgCache(const MsgCache &) = delete;
        MsgCache &operator=(const MsgCache &) = delete;
        MsgCache(MsgCache &&) = delete;
        MsgCache &operator=(MsgCache &&) = delete;

        /**
         * Set the containers to use for caching.
         * @param cache The cache container.
         * @param lru The LRU container.
         */
        void set_containers(Cache* cache, Lru* lru)
        {
            CHECK(cache);
            CHECK(lru);
            _cache = cache;
            _lru = lru;
        }

        /**
         * Cache the string message.
         * @param db The DB ID.
         * @param obj_id The object ID.
         * @param xid The XID/LSN pair.
         * @param msg The message to cache. Usually it's a serialized proto message.
         * @param drop_object Mark the object as dropped,
         * @return true if the element has been actually inserted
         *         and false if it was already in the cache.
         */
        bool insert(DbId db, ObjId obj_id, const XidLsn& xid, std::string_view msg, bool drop_object)
        {
            Key k{db, obj_id};

            Message item{_value_alloc};
            item.xid = xid;
            item.dropped = drop_object;

            auto cmp = [](const auto& a, const auto& b) {return a.xid < b.xid;};

            Lock lock(_mutex);

            auto it = _cache->find(k);
            if (it != _cache->end() && std::ranges::binary_search(it->second, item, cmp)) {
                return false;
            }

            bool key_exists = it != _cache->end();

            // get the last cached message for the object
            // and make sure that it wasn't dropped
            if (!drop_object && key_exists && !it->second.empty()) {
                auto msg_it = it->second.end();
                --msg_it;
                if (msg_it->xid < xid) {
                    // we don't resurrect object
                    DCHECK(!msg_it->dropped);
                }
            }

            while (true) {
                try {
                    if (!key_exists) {
                        it = _cache->emplace(k, Messages(_messages_alloc)).first;
                        key_exists = true;
                    }

                    CHECK(it != _cache->end());

                    item.msg.insert(item.msg.end(), msg.data(), msg.data() + msg.size());

                    if (!it->second.empty() && it->second.back().xid < xid) {
                        it->second.push_back(item);
                    } else {
                        it->second.push_back(item);
                        std::ranges::sort(it->second, cmp);
                    }
                    LruKey lk{db, obj_id, xid};
                    _lru->push_front(lk);
                    break;
                } catch (const std::exception&) {

                    // restore invariants
                    if (key_exists) {
                        auto itt = std::ranges::lower_bound(it->second, item,
                                [](const auto& a, const auto& b) { return a.xid < b.xid; } );
                        if (itt != it->second.end()) {
                            it->second.erase(itt);
                        }
                    }

                    auto it_lru = _lru->template get<1>().find(LruKey{db, obj_id, xid});
                    if (it_lru != _lru->template get<1>().end()) {
                        _lru->template get<1>().erase(it_lru);
                    }

                    // if it fails the memory is probably too small.
                    CHECK(!_lru->empty());

                    evict_locked();
                }
            }
            return true;
        }

        /**
         * Get the cached string if present based on a key.
         * @param db The DB ID.
         * @param obj_id The object ID.
         * @param xid The XID/LSN pair.
         * @return The cached string.
         */
        std::optional<std::string> find(DbId db, ObjId obj_id, const XidLsn& xid)
        {
            std::string ret;
            LruKey lk{db, obj_id, xid};

            { //read-only portion
                SharableLock l{_mutex};

                auto it = _cache->find({db, obj_id});
                if (it == _cache->end()) {
                    return {};
                }

                Message item(_value_alloc);
                item.xid = xid;

                auto itt = std::ranges::lower_bound(it->second, item,
                        [](const auto& a, const auto& b) { return a.xid < b.xid; } );
                if (itt == it->second.end()) {
                    return {};
                }

                if (itt->xid != xid) {
                    return {};
                }
                ret = std::string(itt->msg.data(), itt->msg.size());

                //check if the item is near the top of LRU
                size_t top_cnt = static_cast<double>(_lru->size())*0.1; //in the top 10%
                                                                        //Note: if the number of items in LRU "too small" (<10 elements) then
                                                                        //top_cnt=0 and we'll move to the top. It should be fine actually.

                auto& seq_idx = _lru->template get<0>();
                size_t i = 0;
                for (auto it=seq_idx.begin(); i != top_cnt && it != seq_idx.end(); ++it, ++i)
                {
                    if (*it == lk) {
                        return ret;
                    }
                }
            }

            // this will move the element to the LRU front
            // preserving the insertion sequence.
            {
                Lock l{_mutex};

                auto it_lru = _lru->template get<1>().find(lk);
                if (it_lru != _lru->template get<1>().end()) {
                    auto& seq_idx = _lru->template get<0>();
                    seq_idx.relocate(seq_idx.begin(), _lru->template project<0>(it_lru));
                }
            }

            return ret;
        }

        /**
         * Get the number of cached messages.
         */
        size_t size() const
        {
            SharableLock lock{_mutex};
            return _lru->size();
        }

        /**
         * Return all objects that are tracked by the cache.
         * The least used objects will be at the front.
         */
        std::vector<ObjId>
        get_db_objects(DbId db, bool exclude_dropped)
        {
            std::vector<ObjId> r;

            Lock lock(_mutex);

            auto& seq_idx = _lru->template get<0>();
            for (auto const& v: seq_idx) {
                if (v.db == db && std::ranges::find(r, v.obj_id) == r.end()) {
                    if (exclude_dropped) {
                        // check if the object was dropped
                        const auto& it = _cache->find({db, v.obj_id});
                        CHECK(it != _cache->end());
                        CHECK(!it->second.empty());
                        auto msg_it = it->second.end();
                        --msg_it;
                        if (!msg_it->dropped) {
                            r.push_back(v.obj_id);
                        } else {
                            CHECK(msg_it->msg.empty());
                        }
                    } else {
                        r.push_back(v.obj_id);
                    }
                }
            }
            // least used objects are at the front of the list
            std::ranges::reverse(r);

            return r;
        }

        /**
         * Evict the least recently used element from the cache.
         */
        void evict()
        {
            Lock lock(_mutex);
            evict_locked();
        }

        void evict_locked()
        {
            auto key = _lru->back();
            auto it = _cache->find({key.db, key.obj_id});
            CHECK(it != _cache->end());

            Message msg(_value_alloc);
            msg.xid = key.xid;

            auto itt = std::ranges::lower_bound(it->second, msg,
                    [](const auto& a, const auto& b) { return a.xid < b.xid; } );
            CHECK(itt != it->second.end());
            it->second.erase(itt);
            it->second.shrink_to_fit();

            if (it->second.empty()) {
                _cache->erase(it);
            }

            _lru->pop_back();
        }


    protected:
        Mutex& _mutex;
        Messages::allocator_type& _messages_alloc;
        Value::allocator_type& _value_alloc;

        Cache* _cache = nullptr;
        Lru* _lru = nullptr;
    };
}
