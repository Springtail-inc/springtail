#include <unistd.h>
#include <errno.h>
#include <vector>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>
#include <common/filesystem.hh>

#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_xact_log_reader.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>

namespace springtail {

    void
    PgXactLogReader::scan_file(const std::filesystem::path &file, uint64_t committed_xid)
    {
        _open(file);

        bool eof = false;

        while (!eof) {
            // read header
            // 4B message length + 3B magic + 1B Type
            char header[8];
            _stream.read(header, 8);
            if (_stream.eof()) {
                break;
            }

            // read log header
            uint32_t msg_len = *reinterpret_cast<uint32_t *>(header);
            uint8_t magic[3] = {*reinterpret_cast<uint8_t *>(&header[4]),
                                *reinterpret_cast<uint8_t *>(&header[5]),
                                *reinterpret_cast<uint8_t *>(&header[6])};
            uint8_t type = header[7];

            // verify magic
            assert(magic[0] == PgXactLogWriter::PG_XLOG_MAGIC[0] &&
                   magic[1] == PgXactLogWriter::PG_XLOG_MAGIC[1] &&
                   magic[2] == PgXactLogWriter::PG_XLOG_MAGIC[2]);

            std::cout << "Read header: msg_len=" << msg_len << ", type=" << (int)type << std::endl;

            msg_len -= 8; // subtract header length

            // handle log entry type
            switch (type) {
                case PgTransaction::TYPE_STREAM_START:
                    _read_stream_start(msg_len);
                    break;
                case PgTransaction::TYPE_COMMIT:
                    _read_commit(msg_len, committed_xid);
                    break;
                default:
                    SPDLOG_ERROR("Unknown message type: {}", type);
                    throw Error("Unknown message type");
            }
            assert(!_stream.eof());
        }
    }

    void
    PgXactLogReader::scan_all_files(uint64_t committed_xid)
    {
        // find first file
        std::filesystem::path file = fs::find_earliest_modified_file(_base_dir);
        if (file.empty()) {
            return;
        }

        // iterate through all files, finding next file and scanning each
        while (true) {
            scan_file(file, committed_xid);
            file = fs::get_next_file(file, _file_prefix, _file_suffix);
            if (!std::filesystem::exists(file)) {
                break;
            }
        }
    }

    void
    PgXactLogReader::_open(const std::filesystem::path &path)
    {
        _stream.open(path, std::ios::in);
        if (!_stream.is_open()) {
            SPDLOG_ERROR("Error opening file: path={}, errno={}", path.c_str(), errno);
            throw Error("Error opening file for PgLogFile");
        }
    }

    void
    PgXactLogReader::_read_commit(uint32_t msg_len, uint64_t committed_xid)
    {
        // 4B postgres XID + 8B springtail XID +
        // 8B lsn + 8B begin offset + 8B commit offset +
        // 4B path len + path string (starting offset path) +
        // 4B path len + path string (ending offset path)
        // 4B oid count + oid list (8B each oid)
        // 4B xid count + xid list (4B each xid)

        std::vector<char> buffer(msg_len);
        _stream.read(buffer.data(), msg_len);

        // parse the buffer
        int offset = 0;
        uint32_t xid = *reinterpret_cast<uint32_t *>(&buffer[offset]);
        offset += 4;

        uint64_t springtail_xid = *reinterpret_cast<uint64_t *>(&buffer[offset]);
        offset += 8;

        // remove from stream map if we find a commit that matches
        _stream_map.erase(xid);

        // if xid is less than or equal to committed_xid, then skip
        if (springtail_xid <= committed_xid) {
            return;
        }

        PgTransactionPtr xact= std::make_shared<PgTransaction>();

        xact->type = PgTransaction::TYPE_COMMIT;
        xact->xid = xid;
        xact->springtail_xid = springtail_xid;

        xact->xact_lsn = *reinterpret_cast<uint64_t *>(&buffer[offset]);
        offset += 8;

        xact->begin_offset = *reinterpret_cast<uint64_t *>(&buffer[offset]);
        offset += 8;

        xact->commit_offset = *reinterpret_cast<uint64_t *>(&buffer[offset]);
        offset += 8;

        uint32_t begin_path_len = *reinterpret_cast<uint32_t *>(&buffer[offset]);
        offset += 4;

        uint32_t commit_path_len = *reinterpret_cast<uint32_t *>(&buffer[offset]);
        offset += 4;

        uint32_t oid_count = *reinterpret_cast<uint32_t *>(&buffer[offset]);
        offset += 4;

        uint32_t xid_count = *reinterpret_cast<uint32_t *>(&buffer[offset]);
        offset += 4;

        // read in the path strings
        xact->begin_path = std::string(&buffer[offset], begin_path_len);
        offset += begin_path_len;

        xact->commit_path = std::string(&buffer[offset], commit_path_len);
        offset += commit_path_len;

        // read in the oid list
        for (int i = 0; i < oid_count; i++) {
            uint64_t oid = *reinterpret_cast<uint64_t *>(&buffer[offset]);
            xact->oids.insert(oid);
            offset += 8;
        }

        // read in the aborted xid list
        for (int i = 0; i < xid_count; i++) {
            uint32_t xid = *reinterpret_cast<uint32_t *>(&buffer[offset]);
            xact->aborted_xids.insert(xid);
            offset += 4;
        }

        assert(offset == msg_len);

        _xact_list.push_back(xact);

        return;
    }

    void
    PgXactLogReader::_read_stream_start(uint32_t msg_len)
    {
        // 4B postgres XID + 8B LSN + 8B begin offset + 4B path len + path string

        std::vector<char> buffer(msg_len);
        _stream.read(buffer.data(), msg_len);

        PgTransactionPtr xact= std::make_shared<PgTransaction>();

        // parse the buffer
        int offset = 0;
        xact->xid = *reinterpret_cast<uint32_t *>(&buffer[offset]);
        offset += 4;

        xact->xact_lsn = *reinterpret_cast<uint64_t *>(&buffer[offset]);
        offset += 8;

        xact->begin_offset = *reinterpret_cast<uint64_t *>(&buffer[offset]);
        offset += 8;

        uint32_t path_len = *reinterpret_cast<uint32_t *>(&buffer[offset]);
        offset += 4;

        xact->begin_path = std::string(&buffer[offset], path_len);

        xact->type = PgTransaction::TYPE_STREAM_START;

        _stream_map.insert({xact->xid, xact});

        return;
    }
}