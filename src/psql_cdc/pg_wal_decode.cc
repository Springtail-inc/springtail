#include <fstream>
#include <iostream>
#include <thread>

#include <boost/program_options.hpp>

// springtail includes
#include <psql_cdc/pg_repl_msg.hh>

int main(int argc, char* argv[])
{
    std::string file;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("file,f", boost::program_options::value<std::string>(&file)->default_value("wal.log"), "WAL file to process");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    // open wal log file (written by pg_wal_dump)
    std::FILE* f = std::fopen(file.c_str(), "rb");
    if (f == nullptr) {
        std::cerr << "Error opening output file: " << file;
        return -1;
    }

    // init with protocol 1
    springtail::PgReplMsg msg(1);

    void *buffer = nullptr;
    int max_buffer_len = 0;

    char len_buf[4];
    while (true) {
        // read first 4 bytes for length
        int r = std::fread(len_buf, 4, 1, f);
        int32_t len = springtail::recvint32(len_buf);

        if (r <= 0) {
            // eof
            return 0;
        }

        std::cout << "Read buffer of length: " << len << std::endl;

        // see if another buffer is required
        if (len > max_buffer_len) {
            if (buffer != nullptr) {
                std::free(buffer);
            }
            buffer = std::malloc(len);
            max_buffer_len = len;
        }

        // read in the buffer
        r = std::fread(buffer, len, 1, f);
        if (r <= 0) {
            return 0;
        }

        // iterate through the messages
        msg.setBuffer((const char *)buffer, len);
        while (msg.hasNextMsg()) {
            const springtail::PgReplMsgDecoded &decoded_msg = msg.decodeNextMsg();
            std::string s = msg.dumpMsg(decoded_msg);
            std::cout << s;
        }

    }
}