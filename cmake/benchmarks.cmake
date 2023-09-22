# Benchmarks for storage layer

# check for MinIO SDK
SET(minio_requiredlibs)

# find_package(minio-cpp REQUIRED)
find_path(MINIO_INCLUDE_DIR miniocpp/client.h)
include_directories(${MINIO_INCLUDE_DIR})
find_library(MINIO_LIBRARY miniocpp)
link_libraries(${MINIO_LIBRARY})
# list(APPEND minio_requiredlibs miniocpp)

find_package(CURL REQUIRED)
IF(CURL_FOUND)
  INCLUDE_DIRECTORIES(${CURL_INCLUDE_DIRS})
  list(APPEND minio_requiredlibs CURL::libcurl)
ELSE(CURL_FOUND)
  MESSAGE(FATAL_ERROR "Could not find the CURL library and development files.")
ENDIF(CURL_FOUND)

find_package(unofficial-curlpp CONFIG REQUIRED)
list(APPEND minio_requiredlibs unofficial::curlpp::curlpp)

find_package(unofficial-inih CONFIG REQUIRED)
list(APPEND minio_requiredlibs unofficial::inih::inireader)

find_package(nlohmann_json CONFIG REQUIRED)
list(APPEND minio_requiredlibs nlohmann_json::nlohmann_json)

find_package(pugixml CONFIG REQUIRED)
list(APPEND minio_requiredlibs pugixml)

find_package(OpenSSL REQUIRED)
IF(OPENSSL_FOUND)
  INCLUDE_DIRECTORIES(${OPENSSL_INCLUDE_DIR})
  list(APPEND minio_requiredlibs OpenSSL::SSL OpenSSL::Crypto) # bugfix, because libcrypto is not found automatically
ELSE(OPENSSL_FOUND)
  MESSAGE(FATAL_ERROR "Could not find the OpenSSL library and development files.")
ENDIF(OPENSSL_FOUND)

# fmt
find_package(fmt CONFIG REQUIRED)

# boost program options
find_package(Boost COMPONENTS program_options REQUIRED)
list(APPEND minio_requiredlibs Boost::program_options)

# source files
set(MINIO_SOURCES src/benchmarks/minio_bench.cc)
set(FILE_SOURCES src/benchmarks/file_bench.cc)

# benchmarks
add_executable(minio_bench ${MINIO_SOURCES})
target_link_libraries(minio_bench PRIVATE fmt::fmt ${minio_requiredlibs})

add_executable(file_bench ${FILE_SOURCES})

# # include dirs
# target_include_directories(libcdc
#     PUBLIC include
# )

# # libs to link against
# target_link_libraries(libcdc
#     PRIVATE libpq::pq
#     PRIVATE libpqxx::pqxx)
