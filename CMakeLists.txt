cmake_minimum_required(VERSION 2.8.10)
project(payload_test C CXX)

option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)

execute_process(
    COMMAND bash -c "find ${CMAKE_SOURCE_DIR}/../.. -type d -regex \".*output/include$\" | head -n1 | xargs dirname | tr -d '\n'"
    OUTPUT_VARIABLE OUTPUT_PATH
)

set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

include(FindThreads)
include(FindProtobuf)
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER payload.proto)
# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Search for libthrift* by best effort. If it is not found and brpc is
# compiled with thrift protocol enabled, a link error would be reported.
find_library(THRIFT_LIB NAMES thrift)
if (NOT THRIFT_LIB)
    set(THRIFT_LIB "")
endif()

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

find_path(GLOG_INCLUDE_PATH glog/logging.h)
find_library(GLOG_LIBRARY NAMES glog libglog)
if((NOT GLOG_INCLUDE_PATH) OR (NOT GLOG_LIBRARY))
    message(FATAL_ERROR "Fail to find glog")
endif()
include_directories(${GLOG_INCLUDE_PATH})

find_path(PROTOBUF_INCLUDE_PATH google/protobuf/message.h)
find_library(PROTOBUF_LIBRARY NAMES protobuf libprotobuf)
if((NOT PROTOBUF_INCLUDE_PATH) OR (NOT PROTOBUF_LIBRARY))
    message(FATAL_ERROR "Fail to find protobuf")
endif()
include_directories(${PROTOBUF_INCLUDE_PATH})

find_path(LEVELDB_INCLUDE_PATH leveldb/db.h)
find_library(LEVELDB_LIBRARY NAMES leveldb libleveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIBRARY))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

if(EXAMPLE_LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
    if(NOT BRPC_LIB)
        message(FATAL_ERROR "Fail to find brpc")
    endif()
    find_library(BRAFT_LIB NAMES braft)
    if(NOT BRAFT_LIB)
        message(FATAL_ERROR "Fail to find braft")
    endif()
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
    if(NOT BRPC_LIB)
        message(FATAL_ERROR "Fail to find brpc")
    endif()
    find_library(BRAFT_LIB NAMES libbraft.a braft)
    if(NOT BRAFT_LIB)
        message(FATAL_ERROR "Fail to find braft")
    endif()
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OPENSSL_ROOT_DIR
        "/usr/local/opt/openssl"    # Homebrew installed OpenSSL
        )
endif()

find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${LEVELDB_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    ${BRPC_LIB}
    ${BRAFT_LIB}
    dl
    z)

add_executable(payload_test_client client.cpp ${PROTO_SRC} ${PROTO_HEADER})
add_executable(payload_test_server server.cpp ${PROTO_SRC} ${PROTO_HEADER})

target_link_libraries(payload_test_client ${DYNAMIC_LIB})
target_link_libraries(payload_test_server ${DYNAMIC_LIB}) 