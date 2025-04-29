#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <chrono>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <common/init.hh>
#include <common/environment.hh>
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

        static void SetUpTestSuite() {
            auto service_runners = test::get_services(true, false, false);
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));
            springtail_init_test(runners);
        }

        static void TearDownTestSuite() {
            // shutdown server
            LOG_DEBUG(LOG_XID_MGR, "Shutting down server");
            springtail_shutdown();
        }

        void SetUp() override {
            _subscriber = std::make_unique<Subscriber>();
        }

        void TearDown() override {
            if (_threads.size() > 0) {
                _threads.clear();
            }
        }

        void run_clients(int thread_id, int iterations)
        {
            LOG_INFO("Thread: {}, running {} iterations", thread_id, iterations);
            XidMgrClient *client = XidMgrClient::get_instance();
            xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();

            for (int i = 0; i < iterations; i++) {
                uint64_t xid = client->get_committed_xid(1, 0);
                server->commit_xid(1, 1, xid + 1, false);
            }

            LOG_INFO("Thread: {}, finished", thread_id);
        }

        std::vector<std::jthread> _threads;
        std::unique_ptr<Subscriber> _subscriber;
    };

    TEST_F(XidMgr_Test, ThreadedTest)
    {
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
        LOG_INFO("Push count = {}", _subscriber->_push_cnt);
    }

    TEST_F(XidMgr_Test, Subscriber_Test)
    {
        XidMgrClient *client = XidMgrClient::get_instance();
        xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
        server->rotate(1, timestamp.count());
        uint64_t xid = client->get_committed_xid(1, 0);

        server->commit_xid(1, 1, xid + 1, false);
        server->commit_xid(1, 1, xid + 2, false);
        server->commit_xid(1, 1, xid + 3, false);

        sleep(1);

        uint64_t new_xid = client->get_committed_xid(1, 0);
        ASSERT_EQ(_subscriber->_last_xid, new_xid);
        ASSERT_EQ(_subscriber->_db_id, 1);
        ASSERT_EQ(new_xid, xid + 3);

        _subscriber->cancel();
        ASSERT_GE(_subscriber->_last_xid, 1);
    }

} // namespace







