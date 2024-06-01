#include <iostream>

#include <thrift/transport/TSocket.h>

#include <common/logging.hh>

#include <thrift/write_cache/ThriftWriteCache.h>

namespace springtail {

    /**
     * @brief This is the implementation of the ThriftWriteCacheIf that is generated
     *        from the .thrift file.  It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class ThriftWriteCacheService : public thrift::write_cache::ThriftWriteCacheIf
    {
    public:
        ThriftWriteCacheService() = default;

        void ping(thrift::write_cache::Status& _return) override;
        void add_rows(thrift::write_cache::Status& _return, const thrift::write_cache::AddRowsRequest& request) override;
        void list_extents(thrift::write_cache::ListExtentsResponse& _return, const thrift::write_cache::ListExtentsRequest& request) override;
        void get_rows(thrift::write_cache::GetRowsResponse& _return, const thrift::write_cache::GetRowsRequest& request) override;
        void evict_table(thrift::write_cache::Status& _return, const thrift::write_cache::EvictTableRequest& request) override;
        void add_table_change(thrift::write_cache::Status& _return, const thrift::write_cache::TableChange &change) override;
        void get_table_changes(thrift::write_cache::GetTableChangeResponse& _return, const thrift::write_cache::GetTableChangeRequest& request) override;
        void list_tables(thrift::write_cache::ListTablesResponse& _return, const thrift::write_cache::ListTablesRequest& request) override;
        void evict_table_changes(thrift::write_cache::Status& _return, const thrift::write_cache::EvictTableChangesRequest& request) override;
        void set_clean_flag(thrift::write_cache::Status& _return, const thrift::write_cache::SetCleanFlagRequest& request) override;
        void reset_clean_flag(thrift::write_cache::Status& _return, const thrift::write_cache::ResetCleanFlagRequest& request) override;

        void add_mapping(thrift::write_cache::Status &_return, const thrift::write_cache::AddMappingRequest &request) override;
        void set_lookup(thrift::write_cache::Status &_return, const thrift::write_cache::SetLookupRequest &request) override;
        void forward_map(thrift::write_cache::ExtentMapResponse &_return, const thrift::write_cache::ForwardMapRequest &request) override;
        void reverse_map(thrift::write_cache::ExtentMapResponse &_return, const thrift::write_cache::ReverseMapRequest &request) override;
        void expire_map(thrift::write_cache::Status &_return, const thrift::write_cache::ExpireMapRequest &request) override;
    };


    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     */
    class ThriftWriteCacheCloneFactory : virtual public thrift::write_cache::ThriftWriteCacheIfFactory {
        public:
            ~ThriftWriteCacheCloneFactory() override = default;

            /**
             * @brief Override the thrift getHandler call, allows for logging
             * @param connInfo Thrift connection info object
             * @return thrift::write_cache::ThriftWriteCacheIf*
             */
            thrift::write_cache::ThriftWriteCacheIf*
            getHandler(const apache::thrift::TConnectionInfo &connInfo) override
            {
                std::shared_ptr<apache::thrift::transport::TSocket> sock =
                    std::dynamic_pointer_cast<apache::thrift::transport::TSocket>(connInfo.transport);

                SPDLOG_DEBUG("Incoming connection\n");
                SPDLOG_DEBUG("\tSocketInfo: {}\n", sock->getSocketInfo());
                SPDLOG_DEBUG("\tPeerHost: {}\n", sock->getPeerHost());
                SPDLOG_DEBUG("\tPeerAddress: {}\n", sock->getPeerAddress());
                SPDLOG_DEBUG("\tPeerPort: {}\n", sock->getPeerPort());

                return new ThriftWriteCacheService();
            }

            void
            releaseHandler(thrift::write_cache::ThriftWriteCacheIf *handler) override {
                delete handler;
            }
    };
}
