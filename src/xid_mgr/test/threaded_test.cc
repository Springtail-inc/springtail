#include <thread>
#include <chrono>

#include <gtest/gtest.h>

#include <common/common.hh>
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

            XidMgrServer *server = XidMgrServer::get_instance();

            SPDLOG_INFO("Starting server");

            // startup server
            _server_thread = std::thread([&server](){
                server->startup();
            });
        }

        void TearDown() override {
            // wait for clients to finish
            for (auto &t : _threads) {
                t.join();
            }
            // shutdown server

            XidMgrServer *server = XidMgrServer::get_instance();
            server->shutdown();
            _server_thread.join();
        }

        static void run_clients(int thread_id, int iterations)
        {
            SPDLOG_INFO("Thread: {}, running {} iterations", thread_id, iterations);
            for (int i = 0; i < iterations; i++) {
                XidMgrClient *client = XidMgrClient::get_instance();
                uint64_t xid = client->get_committed_xid();
                client->commit_xid(xid + 1);
            }
            SPDLOG_INFO("Thread: {}, finished", thread_id);
        }

        int THREADS = 500;
        int ITERS = 100;

        std::thread _server_thread;
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







