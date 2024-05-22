#pragma once

#include <filesystem>
#include <iostream>

namespace springtail {
    class fs {
    public:
        /**
         * @brief Find the latest modified file in a directory
         * @param directory directory to search
         * @param prefix file name prefix (removed to find number)
         * @param suffix file name suffix (removed to find number)
         * @return std::filesystem::path path to the latest modified file
         */
        static std::filesystem::path
        find_latest_modified_file(const std::filesystem::path& directory,
                                  const std::string& prefix,
                                  const std::string& suffix)
        {
            // Initialize variables to store the latest file and its modification time
            std::filesystem::path latest_file;
            std::filesystem::file_time_type latest_mtime;

            // Iterate through all files in the directory
            for (const auto& entry : std::filesystem::directory_iterator(directory)) {
                // Check if it's a regular file (not a directory or other special type)
                if (!std::filesystem::is_regular_file(entry)) {
                    continue;
                }
                // Get the modification time of the current file
                std::filesystem::file_time_type current_mtime = std::filesystem::last_write_time(entry);

                // If the current file is newer than the previous, update latest_file and latest_mtime
                if (latest_file.empty() || current_mtime > latest_mtime) {
                    latest_file = entry;
                    latest_mtime = current_mtime;
                } else if (current_mtime == latest_mtime) {
                    int current_id = _extract_id_from_file(entry, prefix, suffix);
                    int latest_id = _extract_id_from_file(latest_file, prefix, suffix);
                    if (current_id > latest_id) {
                        latest_file = entry;
                        latest_mtime = current_mtime;
                    }
                }
            }

            // Return the path to the latest modified file, or an empty path if no files were found
            return latest_file;
        }

        /**
         * @brief Find the earliest modified file in a directory
         * @param directory directory to search
         * @param prefix file name prefix (removed to find number)
         * @param suffix file name suffix (removed to find number)
         * @return std::filesystem::path path to the earliest modified file
         */
        static std::filesystem::path
        find_earliest_modified_file(const std::filesystem::path& directory,
                                    const std::string& prefix,
                                    const std::string& suffix)
        {
            // Initialize variables to store the earliest file and its modification time
            std::filesystem::path earliest_file;
            std::filesystem::file_time_type earliest_mtime;

            // Iterate through all files in the directory
            for (const auto& entry : std::filesystem::directory_iterator(directory)) {
                // Check if it's a regular file (not a directory or other special type)
                if (!std::filesystem::is_regular_file(entry)) {
                    continue;
                }
                // Get the modification time of the current file
                std::filesystem::file_time_type current_mtime = std::filesystem::last_write_time(entry);

                // If the current file is newer than the previous, update latest_file and latest_mtime
                if (earliest_file.empty() || current_mtime < earliest_mtime) {
                    earliest_file = entry;
                    earliest_mtime = current_mtime;
                } else if (current_mtime == earliest_mtime) {
                    int current_id = _extract_id_from_file(entry, prefix, suffix);
                    int earliest_id = _extract_id_from_file(earliest_file, prefix, suffix);
                    if (current_id < earliest_id) {
                        earliest_file = entry;
                        earliest_mtime = current_mtime;
                    }
                }
            }

            // Return the path to the earliest modified file, or an empty path if no files were found
            return earliest_file;
        }

        /**
         * @brief Given a file path, suffix and prefix, increment the number in the file name
         * @param path   file path with filename like: prefix<number>suffix
         * @param prefix file name prefix (removed to find number)
         * @param suffix file name suffix (removed to find number)
         * @return std::filesystem::path new path with incremented number
         */
        static std::filesystem::path
        get_next_file(const std::filesystem::path& path,
                      const std::string& prefix,
                      const std::string& suffix)
        {
            int number = _extract_id_from_file(path, prefix, suffix);
            number++;

            // Add the prefix and suffix back to the file variable
            std::string file = prefix + std::to_string(number) + suffix;

            // Reconstruct the path with the modified file name
            std::filesystem::path modified_path = path;
            modified_path.replace_filename(file);

            return modified_path;
        }
    private:
        static int
        _extract_id_from_file(const std::filesystem::path& path,
                              const std::string& prefix,
                              const std::string& suffix)
        {
            std::string file = path.filename().string();

            // Remove the prefix and suffix from the file variable
            size_t prefix_length = prefix.length();
            size_t suffix_length = suffix.length();
            if (file.substr(0, prefix_length) == prefix) {
                file = file.substr(prefix_length);
            }
            if (file.substr(file.length() - suffix_length) == suffix) {
                file = file.substr(0, file.length() - suffix_length);
            }

            // Convert the remaining part to a number, increment that number, and reconstruct the file name
            return std::stoi(file);
        }
    };
} // namespace springtail
