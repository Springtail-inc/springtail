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
        // if we are processing a new file create a new file stream
        if (_stream == nullptr || path != _current_path) {
            _create_stream(path, start_offset);
        }

        // go to starting offset in this stream
        if (start_offset != 0) {
            _stream->seekg(start_offset, std::fstream::beg);
        }
        _current_offset = start_offset;

        // iterate through the set of messages
        for (int i = 0; i < num_messages; i++) {
            PgReplMsgStream::PgTransactionVectorPtr xacts = std::make_shared<std::vector<PgReplMsgStream::PgTransactionPtr>>();

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
            _pg_repl_stream.scan_log(_stream, _current_path, _current_offset,
                                     header.msg_length, header.proto_version,
                                     xacts);

            // enqueue the xactions for the xact logger to send to GC
            _queue->push(xacts);

            _current_offset += header.msg_length;
        }
    }

    void
    PgLogReader::_create_stream(const std::filesystem::path &path, uint64_t offset)
    {
        _current_path = path;
        _current_offset = offset;

        _stream = std::make_shared<std::fstream>(path, std::fstream::in | std::fstream::binary);
    }
}