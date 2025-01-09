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
        int max_connections = Json::get_or<int>(client_json, "connections", 8);
        int port = Json::get_or<int>(server_json, "port", 55061);

        std::string server = Properties::get_xid_mgr_hostname();

        init(server, port, max_connections);
    }

    // exposed client service interface below

    void
    XidMgrClient::ping()
    {
        thrift::xid_mgr::Status result;
        _invoke_with_retries([&result](ThriftClient &c) {
            c.client->ping(result);
        });
        std::cout << "Ping got: " << result.message << std::endl;
        return;
    }

    void
    XidMgrClient::commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_change)
    {
        thrift::xid_mgr::Status result;
        _invoke_with_retries([&result, db_id, xid, has_schema_change](ThriftClient &c) {
            c.client->commit_xid(result, db_id, xid, has_schema_change);
        });
    }

    void
    XidMgrClient::record_ddl_change(uint64_t db_id, uint64_t xid)
    {
        thrift::xid_mgr::Status result;

        _invoke_with_retries([&result, db_id, xid](ThriftClient &c) {
            c.client->record_ddl_change(result, db_id, xid);
        });
    }

    uint64_t
    XidMgrClient::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
    {
        thrift::xid_mgr::xid_t xid = 0;
        _invoke_with_retries([&xid, db_id, schema_xid](ThriftClient &c) {
            xid = c.client->get_committed_xid(db_id, schema_xid);
        });
        return xid;
    }
}
