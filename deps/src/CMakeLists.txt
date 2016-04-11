project(Featherjl)

cmake_minimum_required(VERSION 3.0)

set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")

list(APPEND CMAKE_CXX_FLAGS "-std=c++11")

set(CxxWrap_DIR "~/.julia/v0.4/CxxWrap/deps/usr/lib/cmake")
find_package(CxxWrap)

add_library(featherjl SHARED featherjl.cpp)
target_link_libraries(featherjl CxxWrap::cxx_wrap feather)

install(TARGETS
  featherjl
LIBRARY DESTINATION lib
ARCHIVE DESTINATION lib
RUNTIME DESTINATION lib
INCLUDES DESTINATION include)
