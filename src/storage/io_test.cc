#include <memory>
#include <vector>
#include <iostream>

#include <storage/io.hh>
#include <storage/io_request.hh>
#include <storage/io_pool.hh>

int main(void)
{
    springtail::IOMgr *IOMgr = springtail::IOMgr::getInstance();
    std::shared_ptr<springtail::IOHandle> fh = IOMgr->open("/tmp/testfile", springtail::IOMgr::IO_MODE::APPEND, true);

    const char *data = "hello";
    
    std::cout << "Issuing append: data len = 5, offset = 0\n";
    std::shared_ptr<springtail::IOResponseAppend> r_append = fh->append(data, 5);
    std::cout << "Append: status: " << r_append->status << " offset=" << r_append->offset << std::endl;

    std::cout << "Issuing read: offset = 0\n";
    std::shared_ptr<springtail::IOResponseRead> r_read = fh->read(0);
    std::cout << "Read: status: " << r_read->status << " offset=" << r_read->offset << std::endl;
    
    std::shared_ptr<std::vector<char>> data_ptr = r_read->data[0];

    std::cout << "Data: size=" << data_ptr->size() << std::endl << "[";
    for (int i = 0; i < data_ptr->size(); i++) {
        std::cout << data_ptr->at(0);
    }
    std::cout << "]\n";

    return 0;
}