#pragma once

#include <array>
#include <istream>
#include <streambuf>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

namespace springtail {

    // NOTE: found this solution on stack exchange website and massaged it a little bit to get it to build

    /**
     * @brief This class runs specified command when it is constructed and stores all output from this
     *      in its internal string _output.
     *
     */
    class execbuf : public std::streambuf {
    protected:
        std::string _output;        ///< string that stores the output of the command
        /**
         * @brief This function returns more data if more data is available in the pipe, otherwise
         *          it returns end of file.
         *
         * @return int_type
         */
        int_type underflow() override
        {
            if (gptr() < egptr()) {
                return traits_type::to_int_type(*gptr());
            }
            return traits_type::eof();
        }
        /**
         * @brief Helper struct that implements custom deleter for FILE
         *
         */
        struct close_pipe_deleter {
            /**
             * @brief Operator that closes file pointer
             *
             * @param file - file pointer to close
             */
            void operator()(FILE* file) const {
                pclose(file);
            }
        };

    public:
        /**
         * @brief Constructor for execbuf object
         *
         * @param command - command for popen
         */
        explicit execbuf(const char* command)
        {
            // create array of specified length
            std::array<char, 128> buffer;
            // create custom deleter object on stack
            close_pipe_deleter pipe_deleter;
            // create unique pointer with a custom deleter to FILE pointer that reads from pipe output
            //  after specified command is run
            std::unique_ptr<FILE, close_pipe_deleter> pipe(popen(command, "r"), pipe_deleter);
            if (!pipe) {
                // throw exception if pipe creation failed; this can happen when command is invalid
                throw std::runtime_error("popen() failed!");
            }
            // read pipe output and store it all in internal buffer
            while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
                _output += buffer.data();
            }
            // if there was an error reading from pipe, throw an exception
            if (ferror(pipe.get())) {
                throw std::runtime_error("Error reading from pipe!");
            }
            // set input sequence pointers
            setg((char*)_output.data(), (char*)_output.data(), (char*)(_output.data() + _output.size()));
        }
    };

    /**
     * @brief This class implements input stream for the command
     *
     */
    class exec : public std::istream {
        protected:
            execbuf _buffer;    ///< custom streambuf object created from the command output
        public:
            /**
             * @brief Construct exec object for the command
             *
             * @param command - command to be executed
             */
            explicit exec(const char* command) : std::istream(nullptr), _buffer(command)
            {
                // set read streambuffer to command output stream buffer
                rdbuf(&_buffer);
            }
    };

    /**
     * @brief Get output of a command and return it
     *
     * @param command - command to be executed
     * @return std::string - command output
     */
    static inline std::string get_command_output(const std::string &command) {
        std::string result;
        std::string line;

        // create command stream
        exec exec_stream(command.c_str());

        // read everything into a string
        while (std::getline(exec_stream, line)) {
            result += line;
        }
        return result;
    }
}