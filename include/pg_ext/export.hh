#pragma once

#ifdef _WIN32
  #define PGEXT_API __declspec(dllexport)
#else
  #define PGEXT_API __attribute__((visibility("default")))
#endif
