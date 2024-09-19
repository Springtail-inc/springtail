#pragma once

#include <common/exception.hh>

namespace springtail {

    /** parent connection error */
    class PgConnectionError : public Error {
    public:
        PgConnectionError() { }
        PgConnectionError(const std::string &error)
            : Error(error)
        { }
    };

    class PgNotConnectedError : public PgConnectionError {
        const char *what() const noexcept {
            return "The connection is closed";
        }
    };

    class PgAlreadyConnectedError : public PgConnectionError {
        const char *what() const noexcept {
            return "Already connected";
        }
    };

    class PgIOError : public PgConnectionError {
        const char *what() const noexcept {
            return "An IO error occurred";
        }
    };

    class PgTableNotFoundError: public PgConnectionError {
        const char *what() const noexcept {
            return "Table not found";
        }
    };

    class PgQueryError : public PgConnectionError {
        const char *what() const noexcept {
            return "An error occurred executing the query";
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

    class PgNoResultError : public PgConnectionError {
        const char *what() const noexcept {
            return "No query result found";
        }
    };


    class PgMessageError : public Error {
    public:
        PgMessageError() { }
        PgMessageError(const std::string &error)
            : Error(error)
        { }
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