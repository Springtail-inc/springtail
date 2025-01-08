#include <nlohmann/json.hpp>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>

#include <common/properties.hh>
#include <common/logging.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/common.hh>
#include <common/exception.hh>

#include <thrift/xid_mgr/ThriftXidMgr.h>

#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_client_factory.hh>

namespace springtail {

    XidMgrClient::XidMgrClient()
    {
        nlohmann::json json = Properties::get(Properties::XID_MGR_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        // fetch properties for the write cache client
        if (!Json::get_to(json, "client", client_json)) {
            throw Error("XID Mgr settings not found");
        }

        if (!Json::get_to(json, "server", server_json)) {
            throw Error("XID Mgr server settings not found");
        }

        // init channel pool
        int max_connections;
        int port;
        Json::get_to<int>(client_json, "connections", max_connections, 8);
        Json::get_to<int>(server_json, "port", port, 55061);

        std::string server = Properties::get_xid_mgr_hostname();

        // construct the thrift client pool.
        // First argument is a factory object that constructs a thrift clients
        // using the host and port from above
        _thrift_client_pool = std::make_shared<ObjectPool<thrift::xid_mgr::ThriftXidMgrClient>>(
            std::make_shared<XidMgrThriftObjectFactory>(server, port),
            max_connections/2,
            max_connections,
            ObjectPool<thrift::xid_mgr::ThriftXidMgrClient>::LIFO
        );
    }

    void XidMgrClient::_reconnect_client(ThriftClient &c) {
        std::shared_ptr<apache::thrift::protocol::TProtocol> proto = c.client->getOutputProtocol();
        std::shared_ptr<apache::thrift::transport::TTransport> trans = proto->getTransport();
        apache::thrift::transport::TFramedTransport *framed_transport = (apache::thrift::transport::TFramedTransport *)trans.get();
        std::shared_ptr<apache::thrift::transport::TTransport> another_transport = framed_transport->getUnderlyingTransport();
        apache::thrift::transport::TSocket *socket = (apache::thrift::transport::TSocket *)another_transport.get();
        socket->close();
        _thrift_client_pool->put(c.client);
        c.client = _thrift_client_pool->get();
    }

    // exposed client service interface below

    void
    XidMgrClient::ping()
    {
        ThriftClient c = _get_client();
        thrift::xid_mgr::Status result;

        bool call_successful = false;
        while (!call_successful) {
            try {
                c.client->ping(result);
                call_successful = true;
            } catch (const apache::thrift::transport::TTransportException &e) {
                SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "Failed API call ping: ", e.what());
                _reconnect_client(c);
            }
        }

        std::cout << "Ping got: " << result.message << std::endl;
        return;
    }

    void
    XidMgrClient::commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_change)
    {
        ThriftClient c = _get_client();
        thrift::xid_mgr::Status result;

        bool call_successful = false;
        while (!call_successful) {
            try {
                c.client->commit_xid(result, db_id, xid, has_schema_change);
                call_successful = true;
            } catch (const apache::thrift::transport::TTransportException &e) {
                SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "Failed API call commit_xid: ", e.what());
                _reconnect_client(c);
            }
        }
    }

    void
    XidMgrClient::record_ddl_change(uint64_t db_id, uint64_t xid)
    {
        ThriftClient c = _get_client();
        thrift::xid_mgr::Status result;

        bool call_successful = false;
        while (!call_successful) {
            try {
                c.client->record_ddl_change(result, db_id, xid);
                call_successful = true;
            } catch (const apache::thrift::transport::TTransportException &e) {
                SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "Failed API call record_ddl_change: ", e.what());
                _reconnect_client(c);
            }
        }
    }

    uint64_t
    XidMgrClient::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
    {
        ThriftClient c = _get_client();

        thrift::xid_mgr::xid_t xid = 0;
        bool call_successful = false;
        while (!call_successful) {
            try {
                xid = c.client->get_committed_xid(db_id, schema_xid);
                call_successful = true;
            } catch (const apache::thrift::transport::TTransportException &e) {
                SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "Failed API call get_committed_xid: ", e.what());
                _reconnect_client(c);
            }
        }
        return xid;
    }
}
