#!/usr/bin/env python3
# JSON config override script
# This script merges two JSON files, where the second file overrides the first.
# Usage:
#   ./merge_system_settings.py <base_file> <override_file> <output_file>


import json


def merge_dict(base, override):
    """
    Merges two dictionaries, where the second dictionary overrides the first.
    If a key is missing from the second dictionary, the value from the first is used.
    If a key exists in both dictionaries and both values are dictionaries, we merge them recursively.
    If a key exists in both dictionaries and the values are not dictionaries, the value from the second dictionary is used.

    :param base: The base dictionary.
    :param override: The dictionary with overrides.
    :return: A new dictionary with merged values.
    """
    merged = base.copy()  # Start with a copy of the base dictionary
    for key, value in override.items():
        if key not in merged:
            raise KeyError(f"Key '{key}' in overrides not found in base.")
        # Assert types are the same if both exist
        base_value = merged[key]
        bvt, ovt = type(base_value), type(value)
        if bvt != ovt:
            raise TypeError(f"Type mismatch for key '{key}': base type {bvt} does not match override type {ovt}.")
        if bvt is dict:
            # If both are dictionaries, merge them recursively
            merged[key] = merge_dict(base_value, value)
        else:
            # Otherwise, override the value
            merged[key] = value
    return merged


def merge_system_settings(base_file, override_file, output_file):
    """
    Merges two YAML files containing system settings, where the second file overrides the first.

    :param base_file: Path to the base system settings YAML file.
    :param override_file: Path to the override system settings YAML file.
    :param output_file: Path to save the merged output YAML file.
    """
    with open(base_file, 'r') as f:
        base_settings = json.load(f)

    with open(override_file, 'r') as f:
        override_settings = json.load(f)

    merged_settings = merge_dict(base_settings, override_settings)

    with open(output_file, 'w') as f:
        json.dump(merged_settings, f, indent=4)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Merge system settings JSON files.")
    parser.add_argument("base_file", help="Path to the base system settings file.")
    parser.add_argument("override_file", help="Path to the override system settings file.")
    parser.add_argument("output_file", help="Path to save the merged output file.")

    args = parser.parse_args()

    merge_system_settings(args.base_file, args.override_file, args.output_file)
