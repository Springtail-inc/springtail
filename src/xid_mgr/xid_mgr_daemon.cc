#include <common/common.hh>
#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

int main(int argc, char *argv[]) {
    springtail_init();

    XidMgrServer *server = XidMgrServer::get_instance();

    if (argc > 1) {
        server->commit_xid(std::stoull(argv[1]));
    }

    server->startup();

    return 0;
}