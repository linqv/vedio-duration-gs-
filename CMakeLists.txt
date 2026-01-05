cmake_minimum_required(VERSION 3.16)
project(nautilus_video_duration C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# 编译选项
add_compile_options(-Wall -Wextra -O2 -fPIC)

find_package(PkgConfig REQUIRED)

pkg_check_modules(NAUTILUS REQUIRED libnautilus-extension-4)
pkg_check_modules(GLIB REQUIRED glib-2.0)
pkg_check_modules(GIO REQUIRED gio-2.0)
pkg_check_modules(GMODULE REQUIRED gmodule-2.0)

# ✅ GStreamer + Discoverer(Pbutils)
pkg_check_modules(GST REQUIRED gstreamer-1.0)
pkg_check_modules(GSTPB REQUIRED gstreamer-pbutils-1.0)

add_library(nautilus-video-duration SHARED nautilus-video-duration.c)

target_include_directories(nautilus-video-duration PRIVATE
    ${NAUTILUS_INCLUDE_DIRS}
    ${GLIB_INCLUDE_DIRS}
    ${GIO_INCLUDE_DIRS}
    ${GMODULE_INCLUDE_DIRS}
    ${GST_INCLUDE_DIRS}
    ${GSTPB_INCLUDE_DIRS}
)

target_compile_options(nautilus-video-duration PRIVATE
    ${NAUTILUS_CFLAGS_OTHER}
    ${GLIB_CFLAGS_OTHER}
    ${GIO_CFLAGS_OTHER}
    ${GMODULE_CFLAGS_OTHER}
    ${GST_CFLAGS_OTHER}
    ${GSTPB_CFLAGS_OTHER}
)

target_link_libraries(nautilus-video-duration
    ${NAUTILUS_LIBRARIES}
    ${GLIB_LIBRARIES}
    ${GIO_LIBRARIES}
    ${GMODULE_LIBRARIES}
    ${GST_LIBRARIES}
    ${GSTPB_LIBRARIES}
)

# 输出文件名：nautilus-video-duration.so
set_target_properties(nautilus-video-duration PROPERTIES
    PREFIX ""
    OUTPUT_NAME "nautilus-video-duration"
)

# 目标安装目录：固定到 Nautilus 4 扩展目录
set(NAUTILUS_EXT_DIR "/usr/lib/nautilus/extensions-4" CACHE PATH
    "Nautilus extensions-4 directory"
)

message(STATUS "Install Nautilus extension to: ${NAUTILUS_EXT_DIR}")

install(TARGETS nautilus-video-duration
    LIBRARY DESTINATION ${NAUTILUS_EXT_DIR}
)
