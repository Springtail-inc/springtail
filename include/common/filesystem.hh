#pragma once

#include <fstream>
#include <filesystem>
#include <functional>
#include <optional>

#include <fmt/format.h>

namespace springtail {
    class fs {
    public:
        /**
         * @brief Find the latest modified file in a directory
         * @param directory directory to search
         * @param prefix file name prefix (removed to find number)
         * @param suffix file name suffix (removed to find number)
         * @return std::optional<std::filesystem::path> path to the latest modified file
         */
        static std::optional<std::filesystem::path>
        find_latest_modified_file(const std::filesystem::path& directory,
                                  const std::string& prefix,
                                  const std::string& suffix)
        {
            // Initialize variables to store the latest file and its timestamp
            std::filesystem::path latest_file;
            std::optional<uint64_t> latest_timestamp;

            // check if directory exists, if not return empty path
            if (!std::filesystem::exists(directory)) {
                return {};
            }

            // Iterate through all files in the directory
            for (const auto& entry : std::filesystem::directory_iterator(directory)) {
                // Check if it's a regular file (not a directory or other special type)
                if (!std::filesystem::is_regular_file(entry)) {
                    continue;
                }

                // Extract the timestamp from the file name
                auto current_timestamp = extract_timestamp_from_file(entry.path(), prefix, suffix);
                if (current_timestamp && (!latest_timestamp || *current_timestamp > *latest_timestamp)) {
                    latest_file = entry.path();
                    latest_timestamp = current_timestamp;
                }
            }

            // Return the path to the latest modified file, or nullopt if no files were found
            if (latest_file.empty()) {
                return std::nullopt;
            }

            return latest_file;
        }

        /**
         * @brief Find the earliest modified file in a directory
         * @param directory directory to search
         * @param prefix file name prefix (removed to find number)
         * @param suffix file name suffix (removed to find number)
         * @return std::optional<std::filesystem::path> path to the earliest modified file
         */
        static std::optional<std::filesystem::path>
        find_earliest_modified_file(const std::filesystem::path& directory,
                                    const std::string& prefix,
                                    const std::string& suffix)
        {
            // Initialize variables to store the earliest file and its timestamp
            std::filesystem::path earliest_file;
            std::optional<uint64_t> earliest_timestamp;

            // check if directory exists, if not return empty path
            if (!std::filesystem::exists(directory)) {
                return {};
            }

            // Iterate through all files in the directory
            for (const auto& entry : std::filesystem::directory_iterator(directory)) {
                // Check if it's a regular file (not a directory or other special type)
                if (!std::filesystem::is_regular_file(entry)) {
                    continue;
                }

                // Extract the timestamp from the file name
                auto current_timestamp = extract_timestamp_from_file(entry.path(), prefix, suffix);
                if (current_timestamp && (!earliest_timestamp || *current_timestamp < *earliest_timestamp)) {
                    earliest_file = entry.path();
                    earliest_timestamp = current_timestamp;
                }
            }

            // Return the path to the earliest modified file, or nullopt if no files were found
            if (earliest_file.empty()) {
                return std::nullopt;
            }

            return earliest_file;
        }

        /**
         * Creates a new log file name using the current timestamp.
         * @param dir The directory in which to create the log file.
         * @param prefix The prefix of the log file name.
         * @param suffix The suffix of the log file name (should contain '.').
         * @return std::filesystem::path Path to a new log file.
         */
        static std::filesystem::path
        create_log_file(const std::filesystem::path &dir,
                        std::string_view prefix,
                        std::string_view suffix)
        {
            auto now = std::chrono::system_clock::now();
            auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
            return dir / fmt::format("{}{}{}", prefix, timestamp.count(), suffix);
        }

        /**
         * @brief Creates a new log file name with the specified timestamp
         * @param dir The directory in which to create the log file.
         * @param prefix The prefix of the log file name.
         * @param suffix The suffix of the log file name (should contain '.').
         * @param timestamp The timestamp to use in the file name
         * @return std::filesystem::path Path to a new log file.
         */
        static std::filesystem::path
        create_log_file_with_timestamp(const std::filesystem::path &dir,
                                       std::string_view prefix,
                                       std::string_view suffix,
                                       uint64_t timestamp)
        {
            return dir / fmt::format("{}{}{}", prefix, timestamp, suffix);
        }

        /**
         * @brief Creates an empty file with prefix, suffix, and timestamp
         * @param dir The directory in which to create rhe empty file.
         * @param prefix The prefix of the empty file name.
         * @param suffix The suffix of the the file name (should contain '.').
         * @param timestamp The timestamp to use in the file name
         */
        static void
        create_empty_file_with_timestamp(const std::filesystem::path &dir,
                                         std::string_view prefix,
                                         std::string_view suffix,
                                         uint64_t timestamp)
        {
            std::filesystem::path file_path = dir / fmt::format("{}{}{}", prefix, timestamp, suffix);
            std::ofstream output(file_path);
        }

        /**
         * @brief Verifies that an empty file exists
         * @param path The directory in which the files are located
         * @param prefix The prefix of the log file.
         * @param ts_prefix The prefix of the empty file.
         * @param suffix The suffix of both files.
         * @return true File exists
         * @return false File does not exists
         */
        static bool
        timestamp_file_exists(
                    const std::filesystem::path &path,
                    std::string_view prefix,
                    std::string_view ts_prefix,
                    std::string_view suffix)
        {
            auto timestamp = extract_timestamp_from_file(path.filename(), prefix, suffix);
            if (!timestamp.has_value()) {
                return false;
            }

            std::filesystem::path file_path = path.parent_path() / fmt::format("{}{}{}", ts_prefix, timestamp.value(), suffix);
            return std::filesystem::exists(file_path);
        }

        /**
         * @brief Given a log file path, finds the next log file in sequence.  Assumes that the log
         *        files are in a directory together and that the files have been constructed using
         *        create_log_file().
         * @param path   file path with filename like: prefix<timestamp>suffix
         * @param prefix the prefix portion of the path
         * @param suffix the suffix portion of the path
         * @return An std::optional containing the path to the next log file in the directory, or
         *         empty if there is no such log file.
         */
        static std::optional<std::filesystem::path>
        get_next_log_file(const std::filesystem::path& path,
                          std::string_view prefix,
                          std::string_view suffix)
        {
            std::filesystem::path dir = path.parent_path();
            std::string current_file = path.filename().string();

            // Extract the timestamp from the current log file using the helper function
            auto current_timestamp = extract_timestamp_from_file(path, prefix, suffix);
            if (!current_timestamp) {
                return std::nullopt; // Return empty if the current file format is incorrect
            }

            // Initialize a variable to store the next log file path
            std::optional<std::filesystem::path> next_log_file;
            std::optional<uint64_t> found_timestamp;

            // Iterate through all files in the directory
            for (const auto& entry : std::filesystem::directory_iterator(dir)) {
                // Check if it's a regular file and matches the prefix and suffix
                if (std::filesystem::is_regular_file(entry) &&
                    entry.path().filename().string().find(prefix) == 0 &&
                    entry.path().filename().string().find(suffix) != std::string::npos) {

                    // Extract the timestamp from the file name
                    auto timestamp = extract_timestamp_from_file(entry.path(), prefix, suffix);

                    // Check if this timestamp is greater than the current timestamp
                    if (timestamp && *timestamp > *current_timestamp) {
                        // If it's the first valid next log file found, store it
                        if (!next_log_file || *timestamp < *found_timestamp) {
                            next_log_file = entry.path();
                            found_timestamp = timestamp;
                        }
                    }
                }
            }

            return next_log_file; // Return the path to the next log file, or empty if none found
        }

        /**
         * @brief This function extracts timestamp id from the file nae
         *
         * @param path      - full file path
         * @param prefix    - file prefix
         * @param suffix    - file suffix
         * @return std::optional<uint64_t> - returns timestamp id if it is found in the file name
         */
        static std::optional<uint64_t>
        extract_timestamp_from_file(const std::filesystem::path& path,
                                     std::string_view prefix,
                                     std::string_view suffix)
        {
            std::string file = path.filename().string();
            size_t prefix_length = prefix.length();
            size_t suffix_length = suffix.length();

            // Ensure the file name has the correct prefix and suffix
            if (file.substr(0, prefix_length) == prefix &&
                file.substr(file.length() - suffix_length) == suffix) {
                std::string timestamp_str = file.substr(prefix_length, file.length() - prefix_length - suffix_length);
                return std::stoull(timestamp_str); // Convert to long long for timestamp
            }
            return std::nullopt; // Return nullopt if the format is incorrect
        }

        /**
         * @brief This function removes all the files from directory with the timestamp id
         *          less than given timestamp limit
         *
         * @tparam Compare  - template parameter for comparison function
         * @param dir       - directory path
         * @param prefix    - file prefix
         * @param suffix    - file suffix
         * @param timestamp_limit   - timestamp id limit
         */
        template <typename Compare = std::less<uint64_t>> static void
        cleanup_files_from_dir(const std::filesystem::path& dir,
                               std::string_view prefix,
                               std::string_view suffix,
                               uint64_t timestamp_limit,
                               bool archive = false)
        {
            if (archive && !std::filesystem::exists(dir / "archive")) {
                // create archive directory
                std::filesystem::create_directories(dir / "archive");
            }
            for (const auto& entry : std::filesystem::directory_iterator(dir)) {
                auto path = entry.path();
                auto filename = path.filename();
                // Check if it's a regular file and matches the prefix and suffix
                if (std::filesystem::is_regular_file(path) &&
                    filename.string().find(prefix) == 0 &&
                    filename.string().find(suffix) != std::string::npos) {

                    // Extract the timestamp from the file name
                    auto timestamp = extract_timestamp_from_file(path, prefix, suffix);
                    if (timestamp.has_value() && Compare()(timestamp.value(), timestamp_limit)) {
                        if (!archive) {
                            std::filesystem::remove(path);
                        } else {
                            std::filesystem::rename(path, dir / "archive" / filename);
                        }
                    }
                }
            }
        }
    };
} // namespace springtail
