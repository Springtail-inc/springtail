#include <iostream>
#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <proxy/server.hh>

#include <proxy/auth/md5.h>

using namespace springtail;

int main(int argc, char* argv[])
{
    springtail_init();

    ProxyServerPtr server = std::make_shared<ProxyServer>("127.0.0.1", 8888, 2);
    server->run();
}

