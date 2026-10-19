# Included at the end of librdkafka's `project()`; see
# windows-librdkafka.cmake. The condition is upstream's own, from the
# `set(WITH_OAUTHBEARER_OIDC ON)` its CMakeLists reaches on every platform.
if(WITH_CURL)
  add_compile_definitions(WITH_CURL=1)
  if(WITH_SSL)
    add_compile_definitions(WITH_OAUTHBEARER_OIDC=1)
  endif()
endif()
