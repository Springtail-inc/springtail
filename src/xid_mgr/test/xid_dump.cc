#include <filesystem>
#include <fstream>
#include <iostream>

#include <boost/program_options.hpp>
#include <common/common.hh>
#include <common/properties.hh>
#include <xid_mgr/xid_partition.hh>

using namespace springtail;

void
read_partition_file(const std::filesystem::path &path)
{
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Could not open file " << path << std::endl;
        return;
    }

    char buffer[xid_mgr::Partition::BUFFER_SIZE + 16];
    file.read(buffer, 8192);
    uint64_t magic;
    uint64_t num_entries;

    std::copy_n(buffer, sizeof(magic), reinterpret_cast<char *>(&magic));
    std::copy_n(buffer + sizeof(magic), sizeof(num_entries),
                reinterpret_cast<char *>(&num_entries));

    assert(magic == xid_mgr::Partition::MAGIC_HDR);
    assert(num_entries * 16 <= xid_mgr::Partition::BUFFER_SIZE);

    int off = sizeof(magic) + sizeof(num_entries);
    for (int i = 0; i < num_entries; i++) {
        uint64_t db_id;
        uint64_t xid;
        std::copy_n(buffer + off, sizeof(db_id), reinterpret_cast<char *>(&db_id));
        off += sizeof(db_id);
        std::copy_n(buffer + off, sizeof(xid), reinterpret_cast<char *>(&xid));
        off += sizeof(xid);

        std::cout << "db_id=" << db_id << ", xid=" << xid << std::endl;
    }

    return;
}

int
main(int argc, char **argv)
{
    // open binary file from command line
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <filename>" << std::endl;
        return 1;
    }

    // list all files that start with "partition_"
    std::filesystem::path path(argv[1]);
    // iterate through all files in the directory
    for (const auto &entry : std::filesystem::directory_iterator(path)) {
        if (entry.path().filename().string().find("partition_") == 0) {
            std::cout << "Dumping: " << entry.path() << "\n";
            read_partition_file(entry.path());
        }
    }

    return 0;
}