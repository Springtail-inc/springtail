#include <gtest/gtest.h>
#include <common/common.hh>

#include <ingest/ingest.hh>

using namespace springtail;

namespace {

    class BTree_Test : public testing::Test {
    protected:
        void SetUp() override {
            springtail_init();

            _base_dir = std::filesystem::temp_directory_path() / "test_ingest";
        }
        void TearDown() override {
            // remove any files created during the run
            std::filesystem::remove_all(_base_dir);
        }
        std::filesystem::path _base_dir;
    };
}
