#include <iostream>
#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <proxy/server.hh>

#include <proxy/auth/md5.h>

using namespace springtail;

void populate_test_users(ProxyServerPtr server)
{
    server->add_database("test", "localhost", 5432);

    // add test user for test db with trust
    server->add_user("test", "test");

    // add test user for test db with md5
    std::string username = "test_md5";
    std::string passwd = "test";
    char md5[36]; // md5sum('pwd'+'user') = md5+digest
    pg_md5_encrypt(passwd.c_str(), username.c_str(), strlen(username.c_str()), md5);
    md5[35] = '\0'; // null terminate
    uint32_t salt;
    get_random_bytes((uint8_t*)&salt, 4);
    SPDLOG_DEBUG("Adding MD5 user: {}, md5: {}, salt: {}", username, md5, salt);
    server->add_user("test_md5", "test", md5, salt);

    // add user for test db with scram
    server->add_user("test_scram", "test", "SCRAM-SHA-256$4096:tb3ZKGGBQOq0eocVNWBbrw==$JrwngrAnMVC0BDQqxK6bREhwqi+ngU6ShRUmswgASLI=:8yAuc+PJJZ1L62803po41jTWmZp5JGwquWQZm6SCvsg=");
}

int main(int argc, char* argv[])
{
    springtail_init();

    ProxyServerPtr server = std::make_shared<ProxyServer>("127.0.0.1", 8888, 2);

    populate_test_users(server);

    server->run();
}

