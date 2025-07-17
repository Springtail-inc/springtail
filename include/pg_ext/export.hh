#pragma once

#ifdef _WIN32
  #define PGEXT_API __declspec(dllexport)
#else
  #define PGEXT_API __attribute__((visibility("default")))

  #ifndef HAVE_UINT8
  typedef unsigned char uint8;	/* == 8 bits */
  typedef unsigned short uint16;	/* == 16 bits */
  typedef unsigned int uint32;	/* == 32 bits */
  #endif
#endif
