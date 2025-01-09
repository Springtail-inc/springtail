#pragma once

#include <iostream>
#include <mutex>

#include <common/logging.hh>
#include <common/singleton.hh>

#include <thrift/write_cache/ThriftWriteCache.h>

namespace springtail {

    /**
     * @brief This is the implementation of the ThriftWriteCacheIf that is generated
     *        from the .thrift file.  It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class ThriftWriteCacheService final :
        public thrift::write_cache::ThriftWriteCacheIf,
        public Singleton<ThriftWriteCacheService>
    {
        friend class Singleton<ThriftWriteCacheService>;
    public:
        void ping(thrift::write_cache::Status& _return) override;
        void evict_table(thrift::write_cache::Status& _return, const thrift::write_cache::EvictTableRequest& request) override;
        void evict_xid(thrift::write_cache::Status& _return, const thrift::write_cache::EvictXidRequest& request) override;
        void list_tables(thrift::write_cache::ListTablesResponse& _return, const thrift::write_cache::ListTablesRequest& request) override;
        void get_extents(thrift::write_cache::GetExtentsResponse& _return, const thrift::write_cache::GetExtentsRequest& request) override;

        void add_mapping(thrift::write_cache::Status &_return, const thrift::write_cache::AddMappingRequest &request) override;
        void set_lookup(thrift::write_cache::Status &_return, const thrift::write_cache::SetLookupRequest &request) override;
        void forward_map(thrift::write_cache::ExtentMapResponse &_return, const thrift::write_cache::ForwardMapRequest &request) override;
        void reverse_map(thrift::write_cache::ExtentMapResponse &_return, const thrift::write_cache::ReverseMapRequest &request) override;
        void expire_map(thrift::write_cache::Status &_return, const thrift::write_cache::ExpireMapRequest &request) override;

    private:
        ThriftWriteCacheService() = default;
        ~ThriftWriteCacheService() override = default;
    };
}
