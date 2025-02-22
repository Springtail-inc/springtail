#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <common/common.hh>
#include <common/json.hh>

#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

namespace {
    /**
     * Framework for Extent testing.
     */
    class XidMgr_Test : public testing::Test {
    protected:
        void SetUp() override {
            springtail_init();

            nlohmann::json json = Properties::get(Properties::XID_MGR_CONFIG);

            std::string base_path_str;
            Json::get_to<std::string>(json, "base_path", base_path_str);
            std::filesystem::path base_path = Properties::make_absolute_path(base_path_str);

            // clear xid directory
            std::filesystem::remove_all(base_path);

            xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();

            SPDLOG_INFO("Starting server");

            // startup server
            server->startup();

            sleep(1);
        }

        void TearDown() override {
            // wait for clients to finish
            for (auto &t : _threads) {
                t.join();
            }
            // shutdown client
            XidMgrClient::shutdown();
            // shutdown server
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Shutting down server");
            xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
            server->shutdown();
        }

        static void run_clients(int thread_id, int iterations)
        {
            SPDLOG_INFO("Thread: {}, running {} iterations", thread_id, iterations);
            for (int i = 0; i < iterations; i++) {
                XidMgrClient *client = XidMgrClient::get_instance();
                uint64_t xid = client->get_committed_xid(1, 0);
                client->commit_xid(1, xid + 1, false);
            }
            SPDLOG_INFO("Thread: {}, finished", thread_id);
        }

        int THREADS = 500;
        int ITERS = 100;

        std::vector<std::thread> _threads;
    };

    TEST_F(XidMgr_Test, ThreadedTest) {
        // startup clients
        for (int i = 0; i < THREADS; i++) {
            _threads.push_back(std::thread(run_clients, i, ITERS));
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
} // namespace







