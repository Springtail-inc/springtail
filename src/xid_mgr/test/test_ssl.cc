#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <common/environment.hh>
#include <common/init.hh>
#include <common/json.hh>

#include <test/services.hh>

#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

namespace {
    class XidMgr_Test : public testing::Test {

        void SetUp() {
            // override the default config to use SSL
            std::string overrides = "xid_mgr.rpc_config.ssl=true";
            ::setenv(environment::ENV_OVERRIDE, overrides.c_str(), 1);

            auto service_runners = test::get_services(true, false, false);
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));
            runners->emplace_back(std::make_unique<GrpcClientRunner<XidMgrClient>>());
            springtail_init_test(runners);
        }

        void TearDown() {
            springtail_shutdown();
        }
    };

    TEST_F(XidMgr_Test, Ping) {
        XidMgrClient *client = XidMgrClient::get_instance();
        ASSERT_NO_THROW(client->ping());
    }

} // namespace