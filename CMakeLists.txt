cmake_minimum_required(VERSION 3.4)
project(MiniDFS)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.59.0 COMPONENTS system filesystem serialization)

if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    aux_source_directory(. DIR_SRCS)
    add_executable(DFS ${DIR_SRCS})
    target_link_libraries(DFS ${Boost_LIBRARIES})
endif ()
