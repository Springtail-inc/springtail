#include <boost/program_options.hpp>

#include <common/common.hh>
#include <common/filesystem.hh>

#include <pg_repl/pg_msg_stream.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_mgr.hh>

using namespace springtail;
namespace po = boost::program_options;

int main(int argc, char *argv[])
{
    std::string file;
    uint64_t start_offset = 0;

    po::options_description desc("Options");
    desc.add_options()
        ("help,h", "Print help message")
        ("file,f", po::value<std::string>(&file), "File to scan")
        ("offset,o", po::value<uint64_t>(&start_offset)->default_value(0), "Start offset")
        ("all,a", "Scan all files");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // check if we need to print the help message
    if (vm.count("help") || vm.count("file") == 0) {
        std::cout << desc << std::endl;
        return 0;
    }

    bool scan_all_files = vm.count("all") > 0;

    std::set<uint32_t> xids;
    std::set<uint32_t> duplicate_xids;

    std::filesystem::path start_file(file);

    while (!start_file.empty() && std::filesystem::exists(start_file)) {

        PgMsgStreamReader reader(start_file, start_offset, -1);
        std::cout << "\nScanning log: " << start_file << std::endl;

        // consume messages from log; num_messages of -1 means go until eos
        bool eos = false; // end of stream
        while (!eos) {
            bool eob=false; // end of block

            // while not at end of message block (or stream) process
            while (!eob && !eos) {
                // read next message
                PgMsgPtr msg = reader.read_message(reader.ALL_MESSAGES, eos, eob);
                if (msg == nullptr) {
                    continue;
                }

                // dump the message
                std::string msg_str = pg_msg::dump_msg(*msg);
                std::cout << msg_str;
                std::cout << "Msg Offset: " << reader.offset() << std::endl;

                if (msg->msg_type == PgMsgEnum::BEGIN) {
                    // extract xid
                    auto begin_msg = std::get<PgMsgBegin>(msg->msg);
                    if (xids.contains(begin_msg.xid)) {
                        std::cout << "Duplicate xid: " << begin_msg.xid << std::endl;
                        duplicate_xids.insert(begin_msg.xid);
                    }
                    xids.insert(begin_msg.xid);
                }
            }
        }

        if (scan_all_files) {
            start_file = fs::get_next_file(start_file, pg_log_mgr::PgLogMgr::LOG_PREFIX_REPL, pg_log_mgr::PgLogMgr::LOG_SUFFIX);
        } else {
            break;
        }
    }

    std::cout << "\nFound xids: " << xids.size() << std::endl;
    for (auto xid: xids) {
        std::cout << xid << " ";
    }
    std::cout << std::endl;

    if (!duplicate_xids.empty()) {
        std::cout << "\nDuplicate xids found:" << std::endl;
        for (auto xid: duplicate_xids) {
            std::cout << xid << std::endl;
        }
    }

    return 0;
}