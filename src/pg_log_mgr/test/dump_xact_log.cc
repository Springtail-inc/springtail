#include <fmt/format.h>

#include <common/common.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_xact_log_reader.hh>

using namespace springtail;
using namespace springtail::pg_log_mgr;

int
main(int argc, char *argv[])
{
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <log_file>" << std::endl;
        return 1;
    }

    std::filesystem::path log_file = argv[1];

    PgXactLogReader reader(PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX);

    reader.begin(log_file);

    std::vector<PgTransactionPtr> xacts;
    std::set<uint32_t> xids;
    std::set<uint32_t> duplicate_xids;

    int num_xacts = 0;
    do {
        num_xacts = reader.next(100, xacts);
        for (auto x : xacts) {
            switch (x->type) {
                case PgTransaction::TYPE_COMMIT:
                    std::cout << "\nCOMMIT:" << std::endl;
                    break;
                case PgTransaction::TYPE_STREAM_START:
                    std::cout << "\nSTREAM_START:" << std::endl;
                    break;
                case PgTransaction::TYPE_STREAM_ABORT:
                    std::cout << "\nSTREAM_ABORT:" << std::endl;
                    break;
                case PgTransaction::TYPE_PIPELINE_STALL:
                    std::cout << "\nPIPELINE_STALL:" << std::endl;
                    break;
                default:
                    std::cout << "\nUNKNOWN: " << x->type << std::endl;
            }
            std::cout << fmt::format("begin_path: {}, commit_path: {}\n", x->begin_path,
                                     x->commit_path);
            std::cout << fmt::format("begin_offset: {}, commit_offset: {}\n", x->begin_offset,
                                     x->commit_offset);
            std::cout << fmt::format("pg_xid: {}, springtail_xid: {}, xact_lsn: {}\n", x->xid,
                                     x->springtail_xid, x->xact_lsn);

            if (xids.contains(x->xid)) {
                std::cerr << "Duplicate xid: " << x->xid << std::endl;
                duplicate_xids.insert(x->xid);
            }
            xids.insert(x->xid);
        }
        xacts.clear();
    } while (num_xacts == 100);

    std::cout << "\nFound xids: " << xids.size() << std::endl;
    for (auto xid : xids) {
        std::cout << xid << " ";
    }
    std::cout << std::endl;

    if (!duplicate_xids.empty()) {
        std::cout << "\nDuplicate xids found:" << std::endl;
        for (auto xid : duplicate_xids) {
            std::cout << xid << std::endl;
        }
    }
}
