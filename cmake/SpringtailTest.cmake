# Variable to capture the list of unit tests
set(SPRINGTAIL_UNIT_TEST_TARGETS)

# Define a function to simplify adding unit tests
function(springtail_unit_test TARGET)
  cmake_parse_arguments(ARG
    ""                                      # Options
    "WORKING_DIR"                           # One-value keywords
    "SOURCES;INCLUDES;LIBS;LINKER_OPTIONS"  # Multi-value keywords
    ${ARGN}
  )
  add_executable(${TARGET} EXCLUDE_FROM_ALL ${ARG_SOURCES})
  target_include_directories(${TARGET}
    PRIVATE ${ARG_INCLUDES}
  )
  target_link_options(${TARGET}
    PRIVATE ${ARG_LINK_OPTIONS}
  )
  target_link_libraries(${TARGET}
    PRIVATE ${ARG_LIBS}
  )
  add_dsymutil_command(${TARGET})
  gtest_discover_tests(${TARGET}
    WORKING_DIRECTORY ${ARG_WORKING_DIR}
    PROPERTIES FIXTURES_REQUIRED ${TARGET}_fixture
  )

  # Fetch the current list
  get_property(existing_tests GLOBAL PROPERTY SPRINGTAIL_UNIT_TEST_TARGETS)
  if(NOT existing_tests)
    set(existing_tests "")
  endif()

  # Append new target and update global property
  list(APPEND existing_tests ${TARGET})
  set_property(GLOBAL PROPERTY SPRINGTAIL_UNIT_TEST_TARGETS "${existing_tests}")
endfunction()
