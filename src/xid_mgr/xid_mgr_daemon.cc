#include <common/common.hh>
#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

int main(int argc, char *argv[]) {
    springtail_init();

    XidMgrServer *server = XidMgrServer::get_instance();

    server->startup();

    return 0;
}