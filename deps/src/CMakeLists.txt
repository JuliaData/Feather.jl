project(Featherjl)

cmake_minimum_required(VERSION 3.0)

set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib;${CxxWrap_DIR}/../")

list(APPEND CMAKE_CXX_FLAGS "-std=c++11")

find_package(CxxWrap)

add_library(featherjl SHARED featherjl.cpp)
target_link_libraries(featherjl CxxWrap::cxx_wrap feather)

install(TARGETS
  featherjl
LIBRARY DESTINATION lib
ARCHIVE DESTINATION lib
RUNTIME DESTINATION lib
INCLUDES DESTINATION include)
