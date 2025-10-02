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

            springtail_init_test();
            
            // get xid_mgr properties check for cert files
            nlohmann::json json = Properties::get(Properties::LOG_MGR_CONFIG);
            if (json.empty() || !json.contains("rpc_config") || !json["rpc_config"].contains("server_cert")) {
                GTEST_SKIP() << "Missing XidMgr rpc config";
                return;
            }

            std::string ssl_file = json["rpc_config"]["server_cert"];
            if (ssl_file.empty() || !std::filesystem::exists(ssl_file)) {
                GTEST_SKIP() << "Missing XidMgr SSL cert file";
                return;
            }

            test::start_services(true, false, false);
        }

        void TearDown() {
            springtail_shutdown();
        }
    };

    TEST_F(XidMgr_Test, SSLPing) {
        XidMgrClient *client = XidMgrClient::get_instance();
        ASSERT_NO_THROW(client->ping());
    }

} // namespace