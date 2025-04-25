#!/bin/bash

# search search paths for a compiler version that is 13 or higher
# set CC or CXX to the compiler path if found
# return 0 if a compiler is found, 1 otherwise
function find_compiler() {
    program_names=$1
    search_paths="/usr/bin /usr/local/bin /opt/bin /opt/homebrew/bin"

    for program_name in $program_names; do
        found=false  # Flag to track if a program is found

        for path in $search_paths; do
            if [[ -x "$path/$program_name" ]]; then
	            # get version
                program="$path/$program_name"
                out=$($program --version 2>&1)

                if [[ ! "$out" =~ "Apple clang version" ]]; then
                    out=$($program -dumpversion 2>&1)

                    # Extract the major version using parameter expansion and slicing
	                major_version=${out%%.*}

                    if [[ "$major_version" -ge 13 ]]; then
                        echo "Found GCC version 13 or higher: $program"
                        found=true
                        break
                    fi
                fi
            fi
        done
        if [[ $found = true ]]; then
            break
        fi
    done

    if [[ $found = true ]]; then
        if [[ "$program_name" =~ ^gcc.* ]]; then
            echo "Setting CC to $program"
            export CC=$program
        else
            echo "Setting CXX to $program"
            export CXX=$program
        fi
        return 0
    fi
    return 1
}

if [ -e '/.dockerenv' ]; then
    DOCKER=1
else
    DOCKER=0
fi

DIR=external/vcpkg
if [ ! -d ${DIR} ]
then
    if [ $DOCKER -eq 1 ]; then
        echo "Building inside a container; symlinking external dir"
        ln -s /home/dev/external external
    fi

    program_names="gcc  gcc-14"
    if ! find_compiler "$program_names"; then
        echo "No suitable gcc compiler found."
        exit 1
    fi

    program_names="g++  g++-14"
    if ! find_compiler "$program_names"; then
        echo "No suitable g++ compiler found."
        exit 1
    fi

    git clone https://github.com/Microsoft/vcpkg.git "${DIR}"
    cd "$DIR"
    # Locks it down to a specific commit
    # Commit 3db09f58b750b9d097d2eb2223b4dba220ee5275 is latest known working commit
    # The one after it: https://github.com/microsoft/vcpkg/commit/11972bdacefc3eaade76df037b0f5fa112c0d36e
    # breaks the logic to find the installed packages.
    git reset --hard "${VCPKG_COMMIT_HASH:-3db09f58b750b9d097d2eb2223b4dba220ee5275}"
   ./bootstrap-vcpkg.sh
   ./vcpkg integrate install
fi
