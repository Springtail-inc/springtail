#!/usr/bin/env python3
# Json shape check:
# This script checks if the JSON structure of a target file matches the schema defined in a source file.
# It validates that all required keys are present and that no unexpected keys are included.
# Usage:
#   json_shape_check.py <source> <target> [optional-paths]
# Provide the optional paths starting with "." and separated by semicolons.
# E.g., '.fdws;.databases;.redis.password;.redis.db;.redis.config_db;.redis.user;.org;.fs;'
import json
import sys
import os
from collections import defaultdict


def build_paths(obj, parent_path="") -> set[tuple[str]]:
    """
    Recursively walk `obj` (a parsed JSON object) and return all_paths:
    a set of all full paths (dot separated) to each key in the JSON object
    """
    all_paths = set()

    if isinstance(obj, dict):
        for key, val in obj.items():
            current_path = parent_path + "." + key
            # record the full path tuple
            all_paths.add(current_path)
            # recurse for the value
            subpaths = build_paths(val, current_path)
            all_paths.update(subpaths)

    return all_paths


def prefix_in(path, path_list: list[str]):
    """
    Check if `path` matches any paths in the `path_list` by prefix.
    e.g., path = "fdws.abc.me", path_list = {"fdws"} ==> match
    """
    for p in path_list:
        if path.startswith(p):
            return True
    return False


class ValidationError(Exception):
    pass


def validate(schema_paths, target_paths, optional_paths):
    """
    Validates `target_obj` (parsed JSON) using:
      - schema_paths: set of all key-paths from the sample JSON
      - target_paths: set of optional paths
      - optional_paths: set of paths (tuples) that are allowed to be missing
    Raises ValidationError with messages if any rule is violated; otherwise returns None.
    """

    errors = []

    # Sort the optional paths by length
    sorted_optional_paths = sorted(optional_paths, key=lambda s: len(s))

    # RULE-1: Every path in target must exist in all_paths
    for path in target_paths:
        if path not in schema_paths:
            errors.append(f"Unexpected key in target: '{path}'")

    # RULE-2: Every key-path in T that is NOT optional must appear in target
    for path in schema_paths:
        if not prefix_in(path, sorted_optional_paths) and path not in target_paths:
            errors.append(f"Missing required key in target: '{path}'")

    if errors:
        raise ValidationError("\n".join(errors))


def main():
    la = len(sys.argv)
    if la < 3:
        print("Usage: system_settings_check.py <source> <target> [semi-colon-separated-optional-paths]")
        os.exit(1)

    with open(sys.argv[1], "r") as fd:
        src = json.load(fd)
    with open(sys.argv[2], "r") as fd:
        target = json.load(fd)

    schema_paths = build_paths(src)
    target_paths = build_paths(target)

    optional_paths = []
    if la > 3:
        optional_paths = [s.strip() for s in str(sys.argv[3]).split(";") if s.strip()]

    try:
        validate(schema_paths, target_paths, optional_paths)
    except ValidationError as e:
        raise
    else:
        print("Schema check ok")


if __name__ == "__main__":
    main()