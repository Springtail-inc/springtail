#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/xid_mgr/ThriftXidMgr.h>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <nlohmann/json.hpp>
#include <xid_mgr/xid_mgr_client.hh>

namespace springtail {

XidMgrClient::XidMgrClient()
{
    nlohmann::json json = Properties::get(Properties::XID_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the xid mgr client
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("XID Mgr RPC settings are not found");
    }

    std::string server = Properties::get_xid_mgr_hostname();

    init(server, rpc_json);
}

// exposed client service interface below

void
XidMgrClient::ping()
{
    thrift::xid_mgr::Status result;
    _invoke_with_retries([&result](ThriftClient &c) { c.client->ping(result); });
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
}  // namespace springtail
