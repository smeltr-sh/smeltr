# Toolchain file for the vendored librdkafka on MSVC. Both settings work
# around the same upstream gap: on `_WIN32`, `rd.h` includes only the
# hand-written `win32_config.h`, never the generated `config.h`, so a cmake
# build has to hand the C code every `WITH_*` macro as a compile definition
# -- and `src/CMakeLists.txt` misses some.
#
# Named for librdkafka and reached by every cmake build in the tree:
# `.cargo/config.toml` can only set the variable for all of them. What each
# one below sets is librdkafka's own and inert elsewhere, but being set at all
# is not -- see the generator that file pins, and why.

# Guards the codec sources, and nothing defaults it ON here.
set(WITH_SNAPPY ON CACHE BOOL "librdkafka: snappy codec")

# `WITH_CURL` and `WITH_OAUTHBEARER_OIDC` are the two macros that list omits,
# so librdkafka compiles the OIDC token refresh and then never wires it up,
# and refuses every `sasl.oauthbearer.*` property. Runs at the end of
# librdkafka's `project()`, before it adds `src`.
set(CMAKE_PROJECT_INCLUDE "${CMAKE_CURRENT_LIST_DIR}/windows-librdkafka-oidc.cmake")
