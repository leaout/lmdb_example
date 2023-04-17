cmake_minimum_required(VERSION 3.10)

project(lmdb_example)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_COMPILER "g++")

add_compile_options(-fopenmp)
add_compile_options(-fpermissive -W -Wall -Wno-unused-parameter -Wuninitialized)

find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if ((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif ()

include_directories(./lmdb/)
aux_source_directory(${CMAKE_SOURCE_DIR}/lmdb LMDB)
add_executable(${PROJECT_NAME} ${CMAKE_SOURCE_DIR}/main.cpp ${LMDB})

target_link_libraries(${PROJECT_NAME} -lpthread -lgflags)