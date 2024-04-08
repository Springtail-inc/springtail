#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <proxy/server.hh>

using namespace springtail;

int main(int argc, char* argv[])
{
    springtail_init();

    ProxyServer server("127.0.0.1", "8888", 4);

    server.run();
}

