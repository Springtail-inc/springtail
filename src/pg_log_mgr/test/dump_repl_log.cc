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
    desc.add_options()("help,h", "Print help message");
    desc.add_options()("file,f", po::value<std::string>(&file), "File to scan");
    desc.add_options()("offset,o", po::value<uint64_t>(&start_offset)->default_value(0), "Start offset");
    desc.add_options()("all,a", "Scan all files");

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

    std::optional<std::filesystem::path> start_file(file);

    while (start_file && std::filesystem::exists(*start_file)) {

        PgMsgStreamReader reader({}, *start_file, start_offset);
        if (start_offset == 0 &&
                    fs::timestamp_file_exists(*start_file,
                                              pg_log_mgr::PgLogMgr::LOG_PREFIX_REPL,
                                              pg_log_mgr::PgLogMgr::LOG_PREFIX_REPL_STREAMING,
                                              pg_log_mgr::PgLogMgr::LOG_SUFFIX)) {
            reader.set_streaming();
        }

        int fsize = std::filesystem::file_size(*start_file);

        // consume messages from log; num_messages of -1 means go until eos
        bool eos = false; // end of stream
        uint64_t last_hdr_offset = 0;
        LSN_t last_lsn = INVALID_LSN;

        while (!eos) {
            // read next message
            auto current_offset = reader.offset();
            std::cout << "Offset: " << current_offset << std::endl;

            PgMsgPtr msg = reader.read_message(reader.ALL_MESSAGES, eos);
            if (msg == nullptr) {
                std::cout << "No more messages or message skipped, eos: " << eos << std::endl;
                continue;
            }

            // get the current xlog header
            auto header = reader.current_header();
            if (header.header_offset != last_hdr_offset) {
                std::cout << std::format("Xlog {}, xlog_end_offset: {}\n",
                                         header.to_string(),
                                         header.msg_length + header.header_offset + PgMsgStreamHeader::SIZE);

                auto hdr_lsn = header.start_lsn;
                if (header.start_lsn != header.end_lsn) {
                    std::cerr << std::format("Warning: start_lsn {} != end_lsn {}\n",
                                             header.start_lsn, header.end_lsn);
                }

                if (last_lsn != INVALID_LSN && hdr_lsn != last_lsn) {
                    std::cout << std::format("LSN jump from {} to {}\n", last_lsn, hdr_lsn);
                }
                last_lsn = header.start_lsn;
            }

            // dump the message
            std::string msg_str = pg_msg::dump_msg(*msg);
            std::cout << msg_str;
            std::cout << "Msg End Offset: " << reader.offset() << std::endl << std::endl;

            if (msg->msg_type == PgMsgEnum::BEGIN) {
                // extract xid
                auto begin_msg = std::get<PgMsgBegin>(msg->msg);
                if (xids.contains(begin_msg.xid)) {
                    std::cout << "Duplicate xid: " << begin_msg.xid << std::endl;
                    duplicate_xids.insert(begin_msg.xid);
                }
                xids.insert(begin_msg.xid);
            }

            // eos is often only set when we actually try to read past the end of the file
            if (reader.offset() == fsize) {
                eos = true;
            }
        }

        if (reader.offset() != fsize) {
            std::cout << std::format("Warning: did not reach end of file, offset: {}, file size: {}\n",
                                     reader.offset(), fsize);
        }

        if (scan_all_files) {
            start_file = fs::get_next_log_file(start_file->parent_path(),
                                               pg_log_mgr::PgLogMgr::LOG_PREFIX_REPL,
                                               pg_log_mgr::PgLogMgr::LOG_SUFFIX);
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
