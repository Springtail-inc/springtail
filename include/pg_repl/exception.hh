#pragma once

#include <common/exception.hh>

namespace springtail {

    class PgUnrecoverableError : public Error{
    public:
        PgUnrecoverableError() = default;
        explicit PgUnrecoverableError(const std::string &error)
            : Error(error)
        { }
    };

    class PgReplicationSlotError : public PgUnrecoverableError {
        const char *what() const noexcept {
            return "Replication slot error";
        }
    };

    class PgIOError : public PgConnectionError {
    public:
        const char *what() const noexcept {
            return "An IO error occurred";
        }
    };

    class PgIOShutdown : public PgConnectionError {
        const char *what() const noexcept {
            return "The connection is being shut down";
        }
    };

    class PgTableNotFoundError: public PgConnectionError {
        const char *what() const noexcept {
            return "Table not found";
        }
    };

    class PgRetryError : public PgConnectionError {
        const char *what() const noexcept {
            return "An error occurred that should result in a retry of the operation";
        }
    };

    class PgStreamingError : public PgConnectionError {
        const char *what() const noexcept {
            return "Error connection is already streaming";
        }
    };

    class PgNotStreamingError : public PgConnectionError {
        const char *what() const noexcept {
            return "Error connection is not streaming";
        }
    };

    class PgCopyDoneError : public PgConnectionError {
        const char *what() const noexcept {
            return "Copy is done";
        }
    };

    class PgMessageError : public Error {
    public:
        PgMessageError() { }
        PgMessageError(const std::string &error)
            : Error(error)
        { }
    };

    class PgMessageEOFError : public PgMessageError {
        const char *what() const noexcept {
            return "Unexpected EOF while reading message";
        }
    };

    class PgMessageTooSmallError : public PgMessageError {
        const char *what() const noexcept {
            return "Can not decode message; data is too small";
        }
    };

    class PgUnexpectedDataError : public PgMessageError {
        const char *what() const noexcept {
            return "Unexepected data found in message";
        }
    };

    class PgUnknownMessageError : public PgMessageError {
        const char *what() const noexcept {
            return "Unknown message type";
        }
    };

    class PgDataOutOfRangeError: public PgMessageError {
        const char *what() const noexcept {
            return "Data out of range";
        }
    };

}
