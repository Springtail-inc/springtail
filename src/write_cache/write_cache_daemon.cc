#include <common/common.hh>

#include <write_cache/write_cache_server.hh>

using namespace springtail;

int main(int argc, char *argv[]) {
    springtail_init();

    WriteCacheServer *server = WriteCacheServer::get_instance();

    server->startup();

    return 0;
}