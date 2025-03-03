#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <common/init.hh>
#include <common/json.hh>

#include <test/services.hh>

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
            std::vector<std::unique_ptr<ServiceRunner>> service_runners = test::get_services(true, false, false);
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));

            springtail_init_test(runners);
        }

        void TearDown() override {
            // wait for clients to finish
            for (auto &t : _threads) {
                t.join();
            }
            // shutdown server
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Shutting down server");
            springtail_shutdown();
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







