#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/dns_resolver.hh>
#include <common/logging.hh>

using namespace springtail;

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

std::optional<std::string>
resolve(const std::string &hostname)
{
    // use gethostbyname to resolve the hostname
    struct hostent *he = gethostbyname(hostname.c_str());
    if (he == nullptr) {
        SPDLOG_ERROR("Failed to resolve hostname {}", hostname);
        return std::nullopt;
    }
    return std::string(inet_ntoa(*((struct in_addr *)he->h_addr)));
}

TEST(DNSTest, SingleThread)
{
    DNSResolver *dns = DNSResolver::get_instance();
    dns->set_ttl_secs(0);

    for (int i = 0; i < 100; i++) {
        std::string hostname = "www.google.com";
        std::optional<std::string> ip = dns->resolve(hostname);
        // std::optional<std::string> ip = resolve(hostname);
        SPDLOG_INFO("{}: Resolved hostname {} to IP {}", i, hostname, ip);
    }
}

TEST(DNSTest, MultithreadTest)
{
    DNSResolver *dns = DNSResolver::get_instance();

    // create multiple threads to test the DNS resolver
    std::vector<std::thread> threads;
    for (int i = 0; i < 100; i++) {
        threads.push_back(std::thread([i, dns]() {
            std::string hostname = "www.google.com";
            std::optional<std::string> ip = dns->resolve(hostname);
            SPDLOG_INFO("{}: Resolved hostname {} to IP {}", i, hostname, ip);
        }));
    }

    for (auto &t : threads) {
        t.join();
    }

    std::cout << "All threads joined." << std::endl;
    DNSResolver::shutdown();
}
