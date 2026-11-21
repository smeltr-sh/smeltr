# Included at the end of librdkafka's `project()`; see
# windows-librdkafka.cmake. The condition is upstream's own, from the
# `set(WITH_OAUTHBEARER_OIDC ON)` its CMakeLists reaches on every platform.
if(WITH_CURL)
  add_compile_definitions(WITH_CURL=1)
  if(WITH_SSL)
    add_compile_definitions(WITH_OAUTHBEARER_OIDC=1)
  endif()
endif()

if(WITH_SSL)
  find_file(SMELTR_WINCRYPT_H wincrypt.h PATHS ENV INCLUDE NO_DEFAULT_PATH REQUIRED)
  file(WRITE "${CMAKE_BINARY_DIR}/smeltr-wincrypt/wincrypt.h" "#pragma once
#include \"${SMELTR_WINCRYPT_H}\"
#undef X509_NAME
#undef X509_EXTENSIONS
#undef X509_CERT_PAIR
#undef PKCS7_ISSUER_AND_SERIAL
#undef PKCS7_SIGNER_INFO
#undef OCSP_REQUEST
#undef OCSP_RESPONSE
")
  include_directories(BEFORE "${CMAKE_BINARY_DIR}/smeltr-wincrypt")
endif()
