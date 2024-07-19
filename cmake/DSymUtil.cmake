# Define a function to simplify adding dsymutil commands
function(add_dsymutil_command TARGET_NAME)
    if(APPLE)
        # Ensure dsymutil is available (it should be on macOS)
        find_program(DSYMUTIL_EXECUTABLE dsymutil)
        if(NOT DSYMUTIL_EXECUTABLE)
            message(FATAL_ERROR "dsymutil not found! Unable to create dSYM files.")
        endif()

        # Define the dSYM file name
        set(DSYM_FILE "${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}.dSYM")

        # Create a custom command to run dsymutil
        add_custom_command(
            TARGET ${TARGET_NAME}
            POST_BUILD
            COMMAND ${DSYMUTIL_EXECUTABLE} $<TARGET_FILE:${TARGET_NAME}> -o ${DSYM_FILE}
            COMMENT "Generating dSYM file for ${TARGET_NAME}"
            VERBATIM
        )
    endif()
endfunction()
