cmake_minimum_required(VERSION 3.16)
project(nautilus_video_duration C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -O2 -fPIC)

find_package(PkgConfig REQUIRED)

pkg_check_modules(NAUTILUS REQUIRED libnautilus-extension-4)
pkg_check_modules(GLIB REQUIRED glib-2.0)
pkg_check_modules(GIO REQUIRED gio-2.0)
pkg_check_modules(GMODULE REQUIRED gmodule-2.0)

# Helper uses FFmpeg libs
pkg_check_modules(AVFORMAT REQUIRED libavformat)
pkg_check_modules(AVUTIL REQUIRED libavutil)

# ---- Nautilus extension (NO ffmpeg link here) ----
add_library(nautilus-video-duration SHARED nautilus-video-duration.c)

target_include_directories(nautilus-video-duration PRIVATE
    ${NAUTILUS_INCLUDE_DIRS}
    ${GLIB_INCLUDE_DIRS}
    ${GIO_INCLUDE_DIRS}
    ${GMODULE_INCLUDE_DIRS}
)

target_compile_options(nautilus-video-duration PRIVATE
    ${NAUTILUS_CFLAGS_OTHER}
    ${GLIB_CFLAGS_OTHER}
    ${GIO_CFLAGS_OTHER}
    ${GMODULE_CFLAGS_OTHER}
)

target_link_libraries(nautilus-video-duration
    ${NAUTILUS_LIBRARIES}
    ${GLIB_LIBRARIES}
    ${GIO_LIBRARIES}
    ${GMODULE_LIBRARIES}
)

set_target_properties(nautilus-video-duration PROPERTIES
    PREFIX ""
    OUTPUT_NAME "nautilus-video-duration"
)

# ---- Helper executable ----
add_executable(vd-ffmpeg-helper vd-ffmpeg-helper.c)

target_include_directories(vd-ffmpeg-helper PRIVATE
    ${AVFORMAT_INCLUDE_DIRS}
    ${AVUTIL_INCLUDE_DIRS}
)

target_compile_options(vd-ffmpeg-helper PRIVATE
    ${AVFORMAT_CFLAGS_OTHER}
    ${AVUTIL_CFLAGS_OTHER}
)

target_link_libraries(vd-ffmpeg-helper
    ${AVFORMAT_LIBRARIES}
    ${AVUTIL_LIBRARIES}
)

# Install paths
set(NAUTILUS_EXT_DIR "/usr/lib/nautilus/extensions-4" CACHE PATH
    "Nautilus extensions-4 directory"
)

message(STATUS "Install Nautilus extension to: ${NAUTILUS_EXT_DIR}")

install(TARGETS nautilus-video-duration
    LIBRARY DESTINATION ${NAUTILUS_EXT_DIR}
)

# Install helper next to extension by default
install(TARGETS vd-ffmpeg-helper
    RUNTIME DESTINATION ${NAUTILUS_EXT_DIR}
)
