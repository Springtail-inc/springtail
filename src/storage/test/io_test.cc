#include <memory>
#include <vector>
#include <iostream>
#include <random>
#include <cassert>

#include <gtest/gtest.h>

#include <storage/io.hh>
#include <storage/io_request.hh>
#include <storage/io_mgr.hh>

#include <common/init.hh>

/**
 * @brief Helper to generate random data
 *
 * @param len  size of generated data
 * @return std::shared_ptr<std::vector<char>>  Ptr to vector of random data
 */
static std::shared_ptr<std::vector<char>>
gen_data(int len)
{
    // set of chars to pick from; too many and data is not compressable
    std::string chars = "ABC0123";
    std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<> dist(0, chars.size()-1);

    std::vector<char> data(len);
    for(int i = 0; i < len; i++){
        int index = dist(generator);
        data[i] = chars[index];
    }

    return std::make_shared<std::vector<char>>(data);
}

/**
 * @brief Compare two data vectors
 *
 * @param data1 First data vector
 * @param data2 Second data vector
 * @return true if vectors contain same data
 * @return false if vectors do not contain same data
 */
static bool
compare_data(std::shared_ptr<std::vector<char>> data1,
             std::shared_ptr<std::vector<char>> data2)
{
    assert(data1->size() == data2->size());

    for (int i = 0; i < data1->size(); i++) {
        if (data1->at(i) != data2->at(i)) {
            return false;
        }
    }
    return true;
}

/**
 * @brief Append data to fh synchronously, then read it back and compare results
 *
 * @param fh_write FH for writing (append)
 * @param fh_read  FH for reading
 * @param len      Length of random data to generate
 * @param count    Number of vectors to generate
 */
static void
sync_append(std::shared_ptr<springtail::IOHandle> fh_write,
            std::shared_ptr<springtail::IOHandle> fh_read,
            int len, int count=1)
{
    std::cout << "\nTesting Sync Append: len=" << len << ", count=" << count << std::endl;

    std::vector<std::shared_ptr<std::vector<char>>> datavec(count);

    for (int i = 0; i < count; i++) {
        std::shared_ptr<std::vector<char>> data = gen_data(len);
        datavec[i] = data;

        if (len <= 16) {
            // dump the data as hex
            char dump[36];
            for (size_t j=0; j < data->size(); j++) {
                sprintf(&dump[j*2], "%02x", (0xFF & (*data)[j]));
            }
            dump[data->size()*2] = 0;
            std::cout << "  - vec[" << i << "] len=" << data->size() << " data=" << dump << std::endl;
        }
    }

    std::shared_ptr<springtail::IOResponseAppend> write_response = fh_write->append(datavec);
    ASSERT_TRUE(write_response->is_success());

    std::shared_ptr<springtail::IOResponse> sync_response = fh_write->sync();
    ASSERT_TRUE(sync_response->is_success());

    std::shared_ptr<springtail::IOResponseRead> read_response = fh_read->read(write_response->offset);
    ASSERT_TRUE(read_response->is_success());
    ASSERT_EQ(read_response->data.size(), count);

    ASSERT_EQ(read_response->next_offset, write_response->next_offset);

    for (int i = 0; i < count; i++) {
        ASSERT_TRUE(compare_data(datavec[i], read_response->data[i]));
    }
}

/**
 * @brief Write (overwrite) data to fh synchronously, then read it back and compare results
 *
 * @param fh_write FH for writing (write)
 * @param fh_read  FH for reading
 * @param len      Length of random data to generate
 * @param count    Number of vectors to generate
 * @param next_offset Out | next offset
 */
static void
sync_write(std::shared_ptr<springtail::IOHandle> fh_write,
           std::shared_ptr<springtail::IOHandle> fh_read,
           int len, uint64_t offset, int count, uint64_t &next_offset)
{
    std::cout << "\nTesting Sync Write: len=" << len << ", offset=" << offset << ", count=" << count << std::endl;

    std::vector<std::shared_ptr<std::vector<char>>> datavec(count);

    for (int i = 0; i < count; i++) {
        std::shared_ptr<std::vector<char>> data = gen_data(len);
        datavec[i] = data;
    }

    std::shared_ptr<springtail::IOResponseWrite> write_response = fh_write->write(offset, datavec);
    ASSERT_TRUE(write_response->is_success());
    ASSERT_EQ(write_response->offset, offset);
    ASSERT_TRUE(write_response->next_offset > (write_response->offset + len));

    std::cout << "  - offset=" << write_response->offset << ", next_offset=" << write_response->next_offset << std::endl;

    std::shared_ptr<springtail::IOResponse> sync_response = fh_write->sync();
    ASSERT_TRUE(sync_response->is_success());

    std::shared_ptr<springtail::IOResponseRead> read_response = fh_read->read(offset);
    ASSERT_TRUE(read_response->is_success());
    ASSERT_EQ(read_response->data.size(), count);
    ASSERT_EQ(read_response->next_offset,write_response->next_offset);

    for (int i = 0; i < count; i++) {
        ASSERT_TRUE(compare_data(datavec[i], read_response->data[i]));
    }

    next_offset = write_response->next_offset;
}

class IOTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        // Init springtail
        springtail::springtail_init_test();
    }

    static void TearDownTestSuite() {
        // cleanup temp files
        for (int i = 0; i < springtail::IOMgr::MAX_FILE_OBJECTS * 2; i++) {
            std::string path = "/tmp/testfile" + std::to_string(i);
            if (std::filesystem::exists(path)) {
                std::filesystem::remove(path);
            }
        }
        springtail::springtail_shutdown();
    }
};

TEST_F(IOTest, FHTests)
{
    int files = (springtail::IOMgr::MAX_FILE_OBJECTS * 2);
    int count = (springtail::IOMgr::MAX_FILE_HANDLES_PER_FILE + 2);
    int data_len = 512;

    std::vector<std::pair<std::string, uint64_t>> file_offsets;

    springtail::IOMgr *IOMgr = springtail::IOMgr::get_instance();

    // open a bunch of files
    for (int i = 0; i < files; i++) {
        std::string path = "/tmp/testfile" + std::to_string(i);
        std::filesystem::remove(path);
        uint64_t offset = 0;

        for (int j = 0; j < count; j++) {
            std::shared_ptr<springtail::IOHandle> fh_write = IOMgr->open(path, springtail::IOMgr::IO_MODE::WRITE, false);
            std::shared_ptr<springtail::IOHandle> fh_read = IOMgr->open(path, springtail::IOMgr::IO_MODE::READ, false);
            file_offsets.push_back(std::make_pair(path, offset));
            sync_write(fh_write, fh_read, data_len, offset, 1, offset);
        }
    }

    // iterate over file offsets and read them
    for (auto &fo : file_offsets) {
        std::string path = fo.first;
        uint64_t offset = fo.second;

        std::shared_ptr<springtail::IOHandle> fh_read = IOMgr->open(path, springtail::IOMgr::IO_MODE::READ, false);
        std::shared_ptr<springtail::IOResponseRead> read_response = fh_read->read(offset);
        ASSERT_TRUE(read_response->is_success());
        ASSERT_EQ(read_response->data.size(), 1);
    }

    // cleanup
    for (auto &fo : file_offsets) {
        std::string path = fo.first;
        std::filesystem::remove(path);
    }
}

TEST_F(IOTest, IOTests)
{
    const char *FILE1 = "/tmp/testfile1";
    const char *FILE2 = "/tmp/testfile2";

    std::filesystem::remove(FILE1);
    std::filesystem::remove(FILE2);

    springtail::IOMgr *IOMgr = springtail::IOMgr::get_instance();

    // open first file for append
    std::shared_ptr<springtail::IOHandle> fh_append = IOMgr->open(FILE1, springtail::IOMgr::IO_MODE::APPEND, true);
    std::shared_ptr<springtail::IOHandle> fh_read = IOMgr->open(FILE1, springtail::IOMgr::IO_MODE::READ, true);

    sync_append(fh_append, fh_read, 512);

    sync_append(fh_append, fh_read, 8192);

    sync_append(fh_append, fh_read, 15);

    sync_append(fh_append, fh_read, 8192, 3);

    sync_append(fh_append, fh_read, 15, 3);

    // test empty vectors
    sync_append(fh_append, fh_read, 0, 3);

    // open second file for write (overwrite)
    std::shared_ptr<springtail::IOHandle> fh_write = IOMgr->open(FILE2, springtail::IOMgr::IO_MODE::WRITE, false);
    fh_read = IOMgr->open(FILE2, springtail::IOMgr::IO_MODE::READ, false);

    springtail::IOHandle fhr = *fh_read;
    springtail::IOHandle fhw = *fh_write;

    uint64_t offset1, offset2, offset3, unused;
    sync_write(fh_write, fh_read, 512, 0, 1, offset1);

    sync_write(fh_write, fh_read, 1024, offset1, 1, offset2);

    sync_write(fh_write, fh_read, 15, offset2, 1, offset3);

    // overwrite data from previous write
    sync_write(fh_write, fh_read, 1024, offset1, 1, unused);

    sync_write(fh_write, fh_read, 256, offset3, 3, unused);

    // cleanup
    std::filesystem::remove(FILE1);
    std::filesystem::remove(FILE2);
}

TEST_F(IOTest, AsyncAppendTest)
{
    const char *FILE = "/tmp/testfile3";
    std::filesystem::remove(FILE);

    springtail::IOMgr *IOMgr = springtail::IOMgr::get_instance();

    // open file for append and read
    std::shared_ptr<springtail::IOHandle> fh_append = IOMgr->open(FILE, springtail::IOMgr::IO_MODE::APPEND, true);
    std::shared_ptr<springtail::IOHandle> fh_read = IOMgr->open(FILE, springtail::IOMgr::IO_MODE::READ, true);

    const int num_appends = 10;
    std::vector<std::shared_ptr<std::vector<char>>> datavec(num_appends);
    std::vector<std::future<std::shared_ptr<springtail::IOResponseAppend>>> futures(num_appends);
    std::vector<uint64_t> offsets(num_appends);

    // Generate data and start async appends
    for (int i = 0; i < num_appends; i++) {
        // generate a random data length between 15 and 400 bytes
        int random_data_len = rand() % (400 - 15 + 1) + 15;
        datavec[i] = gen_data(random_data_len);
        futures[i] = fh_append->async_append(datavec[i], {});
    }

    // Wait for all futures and collect offsets
    for (int i = 0; i < num_appends; i++) {
        auto response = futures[i].get();
        ASSERT_TRUE(response->is_success());
        offsets[i] = response->offset;
    }

    // Sync to disk
    auto sync_response = fh_append->sync();
    ASSERT_TRUE(sync_response->is_success());

    // Read back and verify each append
    for (int i = 0; i < num_appends; i++) {
        auto read_response = fh_read->read(offsets[i]);
        ASSERT_TRUE(read_response->is_success());
        ASSERT_EQ(read_response->data.size(), 1);
        ASSERT_TRUE(compare_data(datavec[i], read_response->data[0]));
    }

    std::filesystem::remove(FILE);
}
