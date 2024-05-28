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
    class ThriftWriteCacheService : public thrift::ThriftWriteCacheIf
    {
    public:
        ThriftWriteCacheService() = default;

        void ping(thrift::Status& _return) override;
        void add_rows(thrift::Status& _return, const thrift::AddRowsRequest& request) override;
        void list_extents(thrift::ListExtentsResponse& _return, const thrift::ListExtentsRequest& request) override;
        void get_rows(thrift::GetRowsResponse& _return, const thrift::GetRowsRequest& request) override;
        void evict_table(thrift::Status& _return, const thrift::EvictTableRequest& request) override;
        void add_table_change(thrift::Status& _return, const thrift::TableChange &change) override;
        void get_table_changes(thrift::GetTableChangeResponse& _return, const thrift::GetTableChangeRequest& request) override;
        void list_tables(thrift::ListTablesResponse& _return, const thrift::ListTablesRequest& request) override;
        void evict_table_changes(thrift::Status& _return, const thrift::EvictTableChangesRequest& request) override;
        void set_clean_flag(thrift::Status& _return, const thrift::SetCleanFlagRequest& request) override;
        void reset_clean_flag(thrift::Status& _return, const thrift::ResetCleanFlagRequest& request) override;

        void add_mapping(thrift::Status &_return, const thrift::AddMappingRequest &request) override;
        void set_lookup(thrift::Status &_return, const thrift::SetLookupRequest &request) override;
        void forward_map(thrift::ExtentMapResponse &_return, const thrift::ForwardMapRequest &request) override;
        void reverse_map(thrift::ExtentMapResponse &_return, const thrift::ReverseMapRequest &request) override;
        void expire_map(thrift::Status &_return, const thrift::ExpireMapRequest &request) override;
    };


    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     */
    class ThriftWriteCacheCloneFactory : virtual public thrift::ThriftWriteCacheIfFactory {
        public:
            ~ThriftWriteCacheCloneFactory() override = default;

            /**
             * @brief Override the thrift getHandler call, allows for logging
             * @param connInfo Thrift connection info object
             * @return thrift::ThriftWriteCacheIf*
             */
            thrift::ThriftWriteCacheIf*
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
            releaseHandler(thrift::ThriftWriteCacheIf *handler) override {
                delete handler;
            }
    };
}
