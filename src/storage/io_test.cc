#include <memory>
#include <vector>
#include <iostream>
#include <random>
#include <cassert>

#include <storage/io.hh>
#include <storage/io_request.hh>
#include <storage/io_pool.hh>

std::shared_ptr<std::vector<char>>
gen_data(int len)
{
    std::string chars = "ABC"; //"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<> dist(0, chars.size()-1);

    std::vector<char> data(len);
    for(int i = 0; i < len; i++){
        int index = dist(generator);
        data[i] = chars[index];
    }

    return std::make_shared<std::vector<char>>(data);
}

bool
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


std::shared_ptr<springtail::IOResponseAppend>
sync_append(std::shared_ptr<springtail::IOHandle> fh_write,
            std::shared_ptr<springtail::IOHandle> fh_read,
            int len, int count=1)
{
    std::cout << "\nTesting Sync Append: len=" << len << ", count=" << count << std::endl;

    std::vector<std::shared_ptr<std::vector<char>>> datavec(count);

    for (int i = 0; i < count; i++) {
        std::shared_ptr<std::vector<char>> data = gen_data(len);
        datavec[i] = data;
    }
    
    std::shared_ptr<springtail::IOResponseAppend> write_response = fh_write->append(datavec);
    assert(write_response->is_success());

    std::shared_ptr<springtail::IOResponse> sync_response = fh_write->sync();
    assert(sync_response->is_success());

    std::shared_ptr<springtail::IOResponseRead> read_response = fh_read->read(write_response->offset);
    assert(read_response->is_success());
    assert(read_response->data.size() == count);

    assert(read_response->next_offset == write_response->next_offset);

    for (int i = 0; i < count; i++) {
        assert(compare_data(datavec[i], read_response->data[i]));
    }

    return write_response;
}

std::shared_ptr<springtail::IOResponseWrite>
sync_write(std::shared_ptr<springtail::IOHandle> fh_write,
           std::shared_ptr<springtail::IOHandle> fh_read,
           int len, uint64_t offset, int count=1)
{
    std::cout << "\nTesting Sync Write: len=" << len << ", offset=" << offset << ", count=" << count << std::endl;

    std::vector<std::shared_ptr<std::vector<char>>> datavec(count);

    for (int i = 0; i < count; i++) {
        std::shared_ptr<std::vector<char>> data = gen_data(len);
        datavec[i] = data;
    }

    std::shared_ptr<springtail::IOResponseWrite> write_response = fh_write->write(offset, datavec);
    assert(write_response->is_success());
    assert(write_response->offset == offset);
    assert(write_response->next_offset > (write_response->offset + len));

    std::cout << "  - offset=" << write_response->offset << ", next_offset=" << write_response->next_offset << std::endl;

    std::shared_ptr<springtail::IOResponse> sync_response = fh_write->sync();
    assert(sync_response->is_success());

    std::shared_ptr<springtail::IOResponseRead> read_response = fh_read->read(offset);
    assert(read_response->is_success());
    assert(read_response->data.size() == count);
    assert(read_response->next_offset == write_response->next_offset);    

    for (int i = 0; i < count; i++) {
        assert(compare_data(datavec[i], read_response->data[i]));        
    }

    return write_response;
}

const char *FILE1 = "/tmp/testfile";
const char *FILE2 = "/tmp/testfile2";

int main(void)
{
    std::filesystem::remove(FILE1);
    std::filesystem::remove(FILE2);

    springtail::IOMgr *IOMgr = springtail::IOMgr::getInstance();
    std::shared_ptr<springtail::IOHandle> fh_append = IOMgr->open(FILE1, springtail::IOMgr::IO_MODE::APPEND, true);
    std::shared_ptr<springtail::IOHandle> fh_read = IOMgr->open(FILE1, springtail::IOMgr::IO_MODE::READ, true);    

    std::shared_ptr<springtail::IOResponseAppend> append_response = sync_append(fh_append, fh_read, 512);

    append_response = sync_append(fh_append, fh_read, 8192);   

    append_response = sync_append(fh_append, fh_read, 15);       

    append_response = sync_append(fh_append, fh_read, 8192, 3);   

    append_response = sync_append(fh_append, fh_read, 15, 3);  


    std::shared_ptr<springtail::IOHandle> fh_write = IOMgr->open(FILE2, springtail::IOMgr::IO_MODE::WRITE, false);
    fh_read = IOMgr->open(FILE2, springtail::IOMgr::IO_MODE::READ, false);
    
    springtail::IOHandle fhr = *fh_read;
    springtail::IOHandle fhw = *fh_write;

    std::shared_ptr<springtail::IOResponseWrite> write_response = sync_write(fh_write, fh_read, 512, 0);
    uint64_t owrite_offset = write_response->next_offset;

    std::cout << " **resp next offset=" << write_response->next_offset << std::endl;

    write_response = sync_write(fh_write, fh_read, 1024, owrite_offset);

    std::cout << " **resp next offset=" << write_response->next_offset << std::endl;

    write_response = sync_write(fh_write, fh_read, 15, write_response->next_offset);
    uint64_t end_offset = write_response->next_offset;

    write_response = sync_write(fh_write, fh_read, 1024, owrite_offset);    

    write_response = sync_write(fh_write, fh_read, 256, end_offset, 3);    

    std::cout << "All tests passed\n";

    return 0;
}