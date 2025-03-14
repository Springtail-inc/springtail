#include <memory>
#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/constants.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <common/threaded_test.hh>

#include <sys_tbl_mgr/client.hh>
#include "sys_tbl_mgr/shm_cache.hh"
#include <test/services.hh>
#include <storage/schema.hh>
#include <storage/xid.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/shm_cache.hh>
#include <xid_mgr/xid_mgr_client.hh>

#include <pg_fdw/pg_xid_subscriber_mgr.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

namespace {

    class XidSubscriber_Test : public testing::Test {
    protected:
        static void SetUpTestSuite() {
            std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
            runners.emplace();
            runners->emplace_back(std::make_unique<IOMgrRunner>());

            auto service_runners = test::get_services(true, true, false);
            std::move(service_runners.begin(), service_runners.end(), std::back_inserter(runners.value()));

            springtail_init_test(runners, LOG_ALL ^ (LOG_STORAGE));

            sys_tbl_mgr::Client *client = sys_tbl_mgr::Client::get_instance();

            // move to the next XID
            ++_xid.xid;

            // create the public namespace in the sys_tbl_mgr
            PgMsgNamespace ns_msg;
            ns_msg.oid = 90000;
            ns_msg.name = "public";
            client->create_namespace(1, _xid, ns_msg);
        }

        static void TearDownTestSuite() {
            springtail_shutdown();
        }

        sys_tbl_mgr::Client *_client = sys_tbl_mgr::Client::get_instance();
        static XidLsn _xid;
    };

    XidLsn XidSubscriber_Test::_xid(1, 0);

    TEST_F(XidSubscriber_Test, Basic)
    {
        uint64_t db = 1;
        uint64_t tid = 200;

        auto xid_mgr = XidMgrClient::get_instance();

        // this will create shm cache asynchronously 
        PgXidSubscriberMgr s(10*1024);

        // this the same cache created by PgXidSubscriber
        std::unique_ptr<sys_tbl_mgr::ShmCache> cache;

        // wait for PgXidSubscriberMgr to create the cache
        for (size_t i = 0; i != 100; ++i) {
            try {
                cache = std::make_unique<sys_tbl_mgr::ShmCache>(sys_tbl_mgr::SHM_CACHE_ROOTS);
                break;
            } catch (...) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        ASSERT_TRUE(cache);


        ++_xid.xid;
        PgMsgTable create_msg;
        create_msg.oid = tid;
        create_msg.namespace_name = "public";
        create_msg.table = "test_table";
        create_msg.columns.push_back({"col1", static_cast<uint8_t>(SchemaType::TEXT), 0, "foo", 1, 0, false, true});
        create_msg.columns.push_back({"col2", static_cast<uint8_t>(SchemaType::INT32), 0, std::nullopt, 2, 0, true, false});
        _client->create_table(db, _xid, create_msg);
        auto &&metadata = _client->get_roots(db, tid, _xid.xid);

        // this 
        xid_mgr->commit_xid(db, _xid.xid, true);

        ++_xid.xid;
        _client->update_roots(db, tid, _xid.xid, {{{0, 1234}}, {17}});
        // commit new xid
        xid_mgr->commit_xid(db, _xid.xid, true);

        // wait for the new xid to be cached by the push
        // notification to PgXidsubscriber
        // Note: we make no direct calls to get_roots()
        for (size_t i = 0; i != 100; ++i) {
            auto r = cache->find(db, tid, _xid.xid);
            if (r) {
                proto::GetRootsResponse response;
                response.ParseFromString(r.value());
                auto a = response.roots().at(0);

                ASSERT_EQ(response.stats().row_count(), 17);
                ASSERT_EQ(a.extent_id(), 1234);
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE(false);
    }
}
