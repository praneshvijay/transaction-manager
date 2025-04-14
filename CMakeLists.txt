# Minimum CMake version required
cmake_minimum_required(VERSION 3.16)

# Project name and version
project(TransactionManager VERSION 1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add source files
set(SOURCES
    mainfile.cpp
    transaction_manager.cpp
)

# Add header files
set(HEADERS
    transaction_manager.hpp
)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Create the executable
add_executable(${PROJECT_NAME} ${SOURCES})

# Link libraries if needed (example)
# target_link_libraries(${PROJECT_NAME} some_library)