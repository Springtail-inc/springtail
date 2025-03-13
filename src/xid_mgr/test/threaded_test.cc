#include <codecvt>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <common/init.hh>
#include <common/json.hh>

#include <test/services.hh>

#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_server.hh>
#include <xid_mgr/xid_mgr_subscriber.hh>

using namespace springtail;

namespace {
    constexpr int THREADS = 500;
    constexpr int ITERS = 100;

    /**
     * Framework for Extent testing.
     */
    class XidMgr_Test : public testing::Test {
    protected:
        struct Subscriber
        {
            Subscriber() 
            {
                XidMgrClient *client = XidMgrClient::get_instance();
                client->ping();

                XidMgrSubscriber::Callbacks cb{
                    [this](uint64_t db, uint64_t xid){ on_push(db, xid); },
                    [this](){on_disconnect();}
                };
                _s = std::make_unique<XidMgrSubscriber>(client->get_channel(), cb);
            }

            void cancel() {
                // GRPC is supposed to delete it after cancel()
                auto p = _s.release();
                p->cancel();
                std::unique_lock<std::mutex> l(_m);
                auto st = _cv_done.wait_for(l, std::chrono::seconds(5), [this]() { return _disconnect; });
                ASSERT_EQ(st, true);
            }

            void on_push(uint64_t db_id, uint64_t xid)
            {
                std::unique_lock<std::mutex> l(_m);
                ++_push_cnt;
                ASSERT_GT(xid, 0);
                _last_xid = xid;
                _db_id = db_id;
            }

            void on_disconnect()
            {
                std::unique_lock<std::mutex> l(_m);
                _disconnect = true;
                _cv_done.notify_one();
            }

            std::unique_ptr<XidMgrSubscriber> _s;
            uint64_t _push_cnt = 0;
            
            std::mutex _m;
            std::condition_variable _cv_done;

            bool _disconnect = false;
            uint64_t _last_xid;
            uint64_t _db_id;
        };

        void SetUp() override {
            auto service_runners = test::get_services(true, false, false);
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));
            runners->emplace_back(std::make_unique<GrpcClientRunner<XidMgrClient>>());
            springtail_init_test(runners);

            _subscriber = std::make_unique<Subscriber>();
        }

        void TearDown() override {
            _threads.clear();
            // shutdown server
            SPDLOG_DEBUG_MODULE(LOG_XID_MGR, "Shutting down server");
            springtail_shutdown();
        }



        void run_clients(int thread_id, int iterations)
        {
            SPDLOG_INFO("Thread: {}, running {} iterations", thread_id, iterations);

            for (int i = 0; i < iterations; i++) {
                XidMgrClient *client = XidMgrClient::get_instance();
                uint64_t xid = client->get_committed_xid(1, 0);
                client->commit_xid(1, xid + 1, false);
            }

            SPDLOG_INFO("Thread: {}, finished", thread_id);
        }

        std::vector<std::jthread> _threads;
        std::unique_ptr<Subscriber> _subscriber;
    };

    TEST_F(XidMgr_Test, ThreadedTest) {
        // startup clients
        for (int i = 0; i < THREADS; i++) {
            _threads.emplace_back([this, i](){run_clients(i, ITERS);});
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        _threads.clear();
        sleep(1);

        XidMgrClient *client = XidMgrClient::get_instance();
        uint64_t xid = client->get_committed_xid(1, 0);
        ASSERT_EQ(_subscriber->_last_xid, xid);
        ASSERT_EQ(_subscriber->_db_id, 1);

        _subscriber->cancel();
        ASSERT_GE(_subscriber->_push_cnt, 200);
    }
} // namespace







