#include <fstream>
#include <iostream>
#include <thread>

#include <boost/program_options.hpp>

// springtail includes
#include <psql_cdc/pg_repl_msg.hh>

void output_msg(const PgReplMsgDecoded &msg)
{
    switch(msg.msg_type) {
        case BEGIN:
            std::cout << "BEGIN";
            std::cout << "xid=" << msg.msg.begin.xid
                      << "; LSN=" << msg.msg.begin.xact_lsn;
            break;

        case COMMIT:
            std::cout << "COMMIT";
            std::cout << "commit LSN=" << msg.msg.commit.commit_lsn
                      << "; xact LSN=" << msg.msg.commit.xact_lsn;
            break;

        case RELATION:
            std::cout << "RELATION";
            std::cout << "rel_id=" << msg.msg.relation.rel_id
                      << "; namespace=" << msg.msg.relation.namespace_str
                      << "; rel_name=" << msg.msg.relation.rel_name_str;
            break;

        case INSERT:
            std::cout << "INSERT";
            std::cout << "rel_id=" << msg.msg.insert.rel_id;
            break;

        case DELETE:
            std::cout << "DELETE";
            std::cout << "rel_id=" << msg.msg.delete_msg.rel_id;
            break;

        case UPDATE:
            std::cout << "UPDATE";
            std::cout << "rel_id=" << msg.msg.update.rel_id;
            break;

        case TRUNCATE:
            std::cout << "TRUNCATE";
            for (int32_t rel_id: msg.msg.truncate.rel_ids) {
                std::cout << "rel_id=" << rel_id;
            }
            break;

        case ORIGIN:
            std::cout << "ORIGIN";
            std::cout << "commit LSN=" << msg.msg.origin.commit_lsn
                      << "; name=" << msg.msg.origin.name_str;
            break;

        case MESSAGE:
            std::cout << "MESSAGE";
            std::cout << "xid=" << msg.msg.message.xid
                      << "; LSN=" << msg.msg.message.lsn
                      << "; prefix=" << msg.msg.message.prefix_str;
            break;

        case TYPE:
            std::cout << "TYPE";
            std::cout << "xid=" << msg.msg.type.xid
                      << "; oid=" << msg.msg.type.oid
                      << "; namespace=" << msg.msg.type.namespace_str;
            break;

        default:
            break;
    }
}

int main(int argc, char* argv[])
{
    std::string file;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("file,o", boost::program_options::value<std::string>(&file)->default_value("wal.log"), "WAL file to process");

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
    PgReplMsg msg(1);

    void *buffer = nullptr;
    int max_buffer_len = 0;

    char len_buf[4];
    while (true) {
        // read first 4 bytes for length
        int r = std::fread(len_buf, 4, 1, f);
        int32_t len = recvint32(len_buf);

        if (r <= 0) {
            // eof
            return 0;
        }

        std::cout << "Read buffer of length: " << len;

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
            const PgReplMsgDecoded &decoded_msg = msg.decodeNextMsg();
            output_msg(decoded_msg);
        }

    }
}