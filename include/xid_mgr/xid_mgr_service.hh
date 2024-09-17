#include <iostream>
#include <mutex>

#include <thrift/transport/TSocket.h>

#include <common/logging.hh>

#include <thrift/xid_mgr/ThriftXidMgr.h>

namespace springtail {

    /**
     * @brief This is the implementation of the ThriftXidMgrIf that is generated
     *        from the .thrift file.  It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class ThriftXidMgrService : public thrift::xid_mgr::ThriftXidMgrIf
    {
    public:
        ThriftXidMgrService() = default;

        /**
         * @brief Get the singleton write cache service instance object
         * @return ThriftWriteCacheService *
         */
        static ThriftXidMgrService *get_instance() {
            std::call_once(_init_flag, &ThriftXidMgrService::_init);
            return _instance;
        }

        /**
         * @brief Shutdown cache
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, &ThriftXidMgrService::_shutdown);
        }

        void ping(thrift::xid_mgr::Status& _return) override;
        void commit_xid(thrift::xid_mgr::Status& _return, const int64_t db_id, const thrift::xid_mgr::xid_t xid, bool has_schema_changes) override;
        void record_ddl_change(thrift::xid_mgr::Status& _return, const int64_t db_id, const thrift::xid_mgr::xid_t xid) override;
        thrift::xid_mgr::xid_t get_committed_xid(const int64_t db_id, thrift::xid_mgr::xid_t schema_xid) override;

    private:
        static ThriftXidMgrService *_instance; ///< singleton instance
        static std::once_flag _init_flag;     ///< init flag
        static std::once_flag _shutdown_flag; ///< shutdown flag

        /** init from get_instance, called once */
        static ThriftXidMgrService *_init();

        /** shutdown from shutdown(), called once */
        static void _shutdown();
    };


    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     */
    class ThriftXidMgrCloneFactory : virtual public thrift::xid_mgr::ThriftXidMgrIfFactory {
        public:
            ~ThriftXidMgrCloneFactory() override = default;

            /**
             * @brief Override the thrift getHandler call, allows for logging
             * @param connInfo Thrift connection info object
             * @return thrift::xid_mgr::ThriftXidMgrIf*
             */
            thrift::xid_mgr::ThriftXidMgrIf*
            getHandler(const apache::thrift::TConnectionInfo &connInfo) override
            {
                std::shared_ptr<apache::thrift::transport::TSocket> sock =
                    std::dynamic_pointer_cast<apache::thrift::transport::TSocket>(connInfo.transport);

                SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Incoming connection");
                SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "\tSocketInfo: {}", sock->getSocketInfo());
                SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "\tPeerHost: {}", sock->getPeerHost());
                SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "\tPeerAddress: {}", sock->getPeerAddress());
                SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "\tPeerPort: {}", sock->getPeerPort());

                return ThriftXidMgrService::get_instance();
            }

            void
            releaseHandler(thrift::xid_mgr::ThriftXidMgrIf *handler) override {
                // delete handler;
            }
    };
}
