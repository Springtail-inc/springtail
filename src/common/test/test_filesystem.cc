#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#include <common/filesystem.hh>

using namespace springtail;

namespace {
    class TestFS : public testing::Test {
    protected:
        void SetUp() override {
            // make a directory in /tmp/
            std::filesystem::create_directory("/tmp/test_fs");

            // create a file in /tmp/test_fs
            std::filesystem::path p = "/tmp/test_fs/test_1.log";
            std::ofstream ofs(p);
            ofs << "test" << std::endl;
            ofs.close();

            // create a second file in /tmp/test_fs
            std::filesystem::path p2 = "/tmp/test_fs/test_2.log";
            std::ofstream ofs2(p2);
            ofs2 << "test" << std::endl;
            ofs2.close();
        }

        void TearDown() override {
            // remove the directory
            std::filesystem::remove_all("/tmp/test_fs");
        }
    };

    TEST_F(TestFS, FindEarliest) {
        auto p = fs::find_earliest_modified_file("/tmp/test_fs", "test_", ".log");
        std::cout << "Earliest:" << *p << std::endl;
        ASSERT_EQ(*p, "/tmp/test_fs/test_1.log");
    }

    TEST_F(TestFS, FindLatest) {
        auto p2 = fs::find_latest_modified_file("/tmp/test_fs", "test_", ".log");
        ASSERT_EQ(*p2, "/tmp/test_fs/test_2.log");
    }

    TEST(FilesystemTest, IncrPath) {
        // create three files in /tmp
        std::filesystem::path p = "/tmp/test_incr_path_1.log";
        std::filesystem::path p2 = "/tmp/test_incr_path_2.log";
        std::filesystem::path p3 = "/tmp/test_incr_path_3.log";

        // Create empty files
        std::ofstream(p).close();  // Creates empty file test_incr_path_1.log
        std::ofstream(p2).close(); // Creates empty file test_incr_path_2.log
        std::ofstream(p3).close(); // Creates empty file test_incr_path_3.log

        std::string prefix = "test_incr_path_";
        std::string suffix = ".log";

        auto find_p2 = fs::get_next_log_file(p, prefix, suffix);
        ASSERT_TRUE(find_p2);
        ASSERT_EQ(*find_p2, "/tmp/test_incr_path_2.log");

        auto find_p3 = fs::get_next_log_file(*find_p2, prefix, suffix);
        ASSERT_TRUE(find_p3);
        ASSERT_EQ(*find_p3, "/tmp/test_incr_path_3.log");

        // Cleanup - remove the files
        std::filesystem::remove(p);
        std::filesystem::remove(p2);
        std::filesystem::remove(p3);
    }
}
