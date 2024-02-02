#include <cassert>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>

namespace springtail {

    void
    PgLogReader::process_log(const std::filesystem::path &path, uint64_t start_offset,
                             uint64_t end_offset, int num_messages)
    {
        std::vector<PgReplMsgStream::PgTransactionPtr> xacts;

        if (_stream == nullptr || path != _current_path) {
            _create_stream(path, start_offset);
        }

        if (start_offset != 0) {
            _stream->seekg(start_offset, std::fstream::beg);
        }
        _current_offset = start_offset;

        // iterate through the set of messages
        for (int i = 0; i < num_messages; i++) {
            // read header
            char buffer[PgLogWriter::PG_LOG_HDR_BYTES];
            _stream->read(buffer, PgLogWriter::PG_LOG_HDR_BYTES);
            _current_offset += PgLogWriter::PG_LOG_HDR_BYTES;

            // decode header
            PgLogWriter::PgLogHeader header;
            PgLogWriter::decode_header(buffer, header);
            // validate magic number
            assert(header.magic == PgLogWriter::PG_LOG_MAGIC);

            // process the log message chunk, get back a list of xactions
            xacts = _pg_repl_stream.scan_log(_stream, _current_path, _current_offset,
                                             header.msg_length, header.proto_version);

            // enqueue the xactions for the GC
            if (!xacts.empty()) {
                _process_xacts(xacts);
            }

            _current_offset += header.msg_length;
        }
    }

    void
    PgLogReader::_process_xacts(const std::vector<PgReplMsgStream::PgTransactionPtr> xacts)
    {

    }

    void
    PgLogReader::_create_stream(const std::filesystem::path &path, uint64_t offset)
    {
        _current_path = path;
        _current_offset = offset;

        _stream = std::make_shared<std::fstream>(path, std::fstream::in | std::fstream::binary);
    }
}