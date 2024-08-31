#include <iostream>
#include <mutex>

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

        /**
         * @brief Get the singleton write cache service instance object
         * @return ThriftWriteCacheService *
         */
        static ThriftWriteCacheService *get_instance() {
            std::call_once(_init_flag, &ThriftWriteCacheService::_init);
            return _instance;
        }

        /**
         * @brief Shutdown cache
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, &ThriftWriteCacheService::_shutdown);
        }

        void ping(thrift::write_cache::Status& _return) override;
        void add_rows(thrift::write_cache::Status& _return, const thrift::write_cache::AddRowsRequest& request) override;
        void list_extents(thrift::write_cache::ListExtentsResponse& _return, const thrift::write_cache::ListExtentsRequest& request) override;
        void get_rows(thrift::write_cache::GetRowsResponse& _return, const thrift::write_cache::GetRowsRequest& request) override;
        void evict_table(thrift::write_cache::Status& _return, const thrift::write_cache::EvictTableRequest& request) override;
        void add_table_change(thrift::write_cache::Status& _return, const thrift::write_cache::AddTableChangeRequest& request) override;
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

    private:
        static ThriftWriteCacheService *_instance; ///< singleton instance
        static std::once_flag _init_flag;     ///< init flag
        static std::once_flag _shutdown_flag; ///< shutdown flag

        /** init from get_instance, called once */
        static ThriftWriteCacheService *_init();

        /** shutdown from shutdown(), called once */
        static void _shutdown();
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

                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "Incoming connection\n");
                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "\tSocketInfo: {}\n", sock->getSocketInfo());
                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "\tPeerHost: {}\n", sock->getPeerHost());
                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "\tPeerAddress: {}\n", sock->getPeerAddress());
                SPDLOG_DEBUG_MODULE(LOG_WRITE_CACHE_SERVER, "\tPeerPort: {}\n", sock->getPeerPort());

                return ThriftWriteCacheService::get_instance();
            }

            void
            releaseHandler(thrift::write_cache::ThriftWriteCacheIf *handler) override {
                // delete handler;
            }
    };
}
