#include <cassert>

#include <common/logging.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>

#include <pg_repl/pg_msg_stream.hh>

namespace springtail {

    void
    PgLogReader::process_log(const std::filesystem::path &path,
                             uint64_t start_offset,
                             int num_messages)
    {
        // init stream reader
        _reader.set_file(path, start_offset);

        std::vector<char> filter = { pg_msg::MSG_BEGIN,
            pg_msg::MSG_COMMIT,
            pg_msg::MSG_STREAM_START,
            pg_msg::MSG_STREAM_COMMIT,
            pg_msg::MSG_STREAM_ABORT,
            pg_msg::MSG_MESSAGE // this will capture create_table, drop_table, alter_table
        };

        _current_path = path;

        // consume messages from log; num_messages of -1 means go until eos
        bool eos = false; // end of stream
        while (num_messages != 0 && !eos) {
            bool eob=false; // end of block

            // while not at end of message block (or stream) process
            while (!eob && !eos) {
                // read next message
                PgMsgPtr msg = _reader.read_message(filter, eos, eob);
                if (msg == nullptr) {
                    continue;
                }

                // handle the message
                switch(msg->msg_type) {
                    case PgMsgEnum::BEGIN:
                        _process_begin(std::get<PgMsgBegin>(msg->msg));
                        break;

                    case PgMsgEnum::COMMIT:
                        _process_commit(std::get<PgMsgCommit>(msg->msg));
                        break;

                    case PgMsgEnum::STREAM_START:
                        _process_stream_start(std::get<PgMsgStreamStart>(msg->msg));
                        break;

                    case PgMsgEnum::STREAM_COMMIT:
                        _process_stream_commit(std::get<PgMsgStreamCommit>(msg->msg));
                        break;

                    case PgMsgEnum::STREAM_ABORT:
                        _process_stream_abort(std::get<PgMsgStreamAbort>(msg->msg));
                        break;

                    case PgMsgEnum::CREATE_TABLE:
                    case PgMsgEnum::ALTER_TABLE:
                    {
                        PgMsgTable &table_msg = std::get<PgMsgTable>(msg->msg);
                        _process_ddl(table_msg.oid, table_msg.xid, msg->is_streaming);
                        break;
                    }
                    case PgMsgEnum::DROP_TABLE:
                    {
                        PgMsgDropTable &drop_msg = std::get<PgMsgDropTable>(msg->msg);
                        _process_ddl(drop_msg.oid, drop_msg.xid, msg->is_streaming);
                        break;
                    }
                    default:
                        SPDLOG_WARN("Unknown message type\n");
                        break;
                }
            }

            if (num_messages > 0) {
                num_messages--;
            }
        }
    }

    void
    PgLogReader::_process_begin(const PgMsgBegin &begin_msg)
    {
        SPDLOG_DEBUG("Begin: xid={}, xact_lsn={}\n", begin_msg.xid, begin_msg.xact_lsn);

        PgTransactionPtr xact = std::make_shared<PgTransaction>();
        xact->begin_path = _current_path;
        xact->begin_offset = _reader.header_offset();
        xact->xact_lsn = begin_msg.xact_lsn;
        xact->xid = begin_msg.xid;
        _current_xact = xact;
    }

    void
    PgLogReader::_process_commit(const PgMsgCommit &commit_msg)
    {
        SPDLOG_DEBUG("Commit: commit_lsn={}, xact_lsn={}\n", commit_msg.commit_lsn, commit_msg.xact_lsn);

        PgTransactionPtr xact = _current_xact;
        if (_current_xact == nullptr || commit_msg.commit_lsn != _current_xact->xact_lsn) {
            // we don't have the start of the transaction...
            SPDLOG_WARN("No matching xact for commit: commit_lsn={}\n", commit_msg.commit_lsn);
            return;
        }

        // set transaction path and end offset
        xact->commit_path = _current_path;
        xact->commit_offset = _reader.offset();

        _queue->push(xact);
        _current_xact = nullptr;
    }

    void
    PgLogReader::_process_stream_start(const PgMsgStreamStart &start_msg)
    {
        SPDLOG_DEBUG("Stream start: xid={}, first={}\n", start_msg.xid, start_msg.first);

        if (!start_msg.first) {
            return;
        }

        // new transaction
        PgTransactionPtr xact = std::make_shared<PgTransaction>();
        xact->begin_path = _current_path;
        xact->begin_offset = _reader.header_offset();
        xact->xid = start_msg.xid;
        _xact_map.insert({xact->xid, xact});

        PgTransactionPtr stream_xact = std::make_shared<PgTransaction>();
        stream_xact->begin_path = _current_path;
        stream_xact->begin_offset = _reader.header_offset();
        stream_xact->xid = start_msg.xid;
        stream_xact->type = PgTransaction::TYPE_STREAM_START;

        _queue->push(stream_xact);
    }

    void
    PgLogReader::_process_stream_commit(const PgMsgStreamCommit &commit_msg)
    {
        SPDLOG_DEBUG("Stream commit: xid={}, xact_lsn={}\n", commit_msg.xid, commit_msg.xact_lsn);

        // commit only happens for the top level xid, subxacts under the xid
        // automatically commit unless they were previously aborted
        auto itr = _xact_map.find(commit_msg.xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            SPDLOG_WARN("No matching xact for stream commit: xid={}, xact_lsn={}",
                        commit_msg.xid, commit_msg.xact_lsn);
            return;
        }

        PgTransactionPtr xact = itr->second;
        xact->commit_path = _current_path;
        xact->commit_offset = _reader.offset();
        xact->xact_lsn = commit_msg.xact_lsn;
        xact->type = PgTransaction::TYPE_COMMIT;

        _xact_map.erase(itr);
        _queue->push(xact);
    }

    void
    PgLogReader::_process_stream_abort(const PgMsgStreamAbort &abort_msg)
    {
        SPDLOG_DEBUG("Stream abort: xid={}, sub_xid={}\n", abort_msg.xid, abort_msg.sub_xid);

        auto itr = _xact_map.find(abort_msg.xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            SPDLOG_WARN("No matching xact for stream abort: xid={}, xact_lsn={}",
                        abort_msg.xid, abort_msg.abort_lsn);
            return;
        }

        if (abort_msg.sub_xid == abort_msg.xid) {
            // if sub_xid == xid, then it's a top level xact that aborted
            // add it to the xact queue for logging
            PgTransactionPtr xact = itr->second;
            xact->type = PgTransaction::TYPE_STREAM_ABORT;
            _queue->push(xact);
            // remove it from the map
            _xact_map.erase(itr);
        } else {
            // subtransaction aborted, add to parent xact aborted list
            itr->second->aborted_xids.insert(abort_msg.sub_xid);
        }

    }

    void
    PgLogReader::_process_ddl(uint32_t oid, int32_t xid, bool is_streaming)
    {
        if (!is_streaming) {
            // not streaming, so xid is invalid; use current_xact
            _current_xact->oids.insert(oid);
            return;
        }

        // if streaming, then xid is valid
        auto itr = _xact_map.find(xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            SPDLOG_WARN("XID not found for message: xid={}\n", xid);
            return;
        }

        PgTransactionPtr xact = itr->second;
        xact->oids.insert(oid);
    }
}
