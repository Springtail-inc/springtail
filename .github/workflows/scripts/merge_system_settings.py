#!/usr/bin/env python3
# JSON config override script
# This script merges two JSON files, where the second file overrides the first.
# Usage:
#   ./merge_system_settings.py <base_file> <override_file> <output_file>

import dataclasses
import json
import os
import re

_VAR = re.compile(r'(?<!\\)\$\{([^}]+)\}')


@dataclasses.dataclass
class VarSpan:
    start: int
    end: int
    name: str
    actualValue: str = None  # to be filled in later


def find_braced_var_spans(s: str) -> list[VarSpan]:
    """
    :param s: input string
    Return a list of dicts: {'start': int, 'end': int, 'name': str}
    - start/end are Python slice indices (end is exclusive)
    - name is the content inside ${...}
    """
    return [VarSpan(start=m.start(), end=m.end(), name=m.group(1))
            for m in _VAR.finditer(s)]


def build_eventual_copy(override: dict) -> dict:
    """
    Recursively builds a copy of the override dictionary, replacing any ${VAR} strings with their actual values from
    environment variables.
    :param override:
    :return:
    """

    def replace_vars_in_string(s: str) -> str:
        spans = find_braced_var_spans(s)
        if not spans:
            return s
        parts = []
        last_index = 0
        for span in spans:
            parts.append(s[last_index:span.start])
            span.actualValue = os.getenv(span.name)
            if span.actualValue is None:
                raise ValueError(f"Environment variable '{span.name}' not set for override.")
            parts.append(span.actualValue)
            last_index = span.end
        parts.append(s[last_index:])
        return ''.join(parts)

    if isinstance(override, dict):
        return {k: build_eventual_copy(v) for k, v in override.items()}
    elif isinstance(override, list):
        return [build_eventual_copy(item) for item in override]
    elif isinstance(override, str):
        return replace_vars_in_string(override)
    else:
        return override


stashed_key_values = {}


def merge_dict(base, override):
    """
    Merges two dictionaries, where the second dictionary overrides the first.
    If a key is missing from the second dictionary, the value from the first is used.
    If a key exists in both dictionaries and both values are dictionaries, we merge them recursively.
    If a key exists in both dictionaries and the values are not dictionaries, the value from the second dictionary is used.

    If a keys starts with '*/<key>', the value is stashed for later use to override all keys' values in the children.

    E.g., given base:
        {
            "a": {
                "b": 1,
                "c": {
                    "b": 2
                }
            },
            "d": 3
        }
        and override:
        {
            "*/b": 10
        }
        the result will be:
        {
            "a": {
                "b": 10,
                "c": {
                    "b": 10
                }
            },
            "d": 3
        }

    :param base: The base dictionary.
    :param override: The dictionary with overrides.
    :return: A new dictionary with merged values.
    """
    merged = base.copy()  # Start with a copy of the base dictionary
    for key, value in override.items():
        if key.startswith('*/'):
            # Stash the value for later use
            stashed_key_values[key[2:]] = value
            continue
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

    # Second pass: apply stashed values to all matching keys by recursively going through the merged dict
    def apply_stashed(m):
        for k, v in m.items():
            if k in stashed_key_values:
                m[k] = stashed_key_values[k]
            elif isinstance(v, dict):
                apply_stashed(v)

    apply_stashed(merged)
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

    override_settings = build_eventual_copy(override_settings)

    merged_settings = merge_dict(base_settings, override_settings)

    if output_file == '-':
        print(json.dumps(merged_settings, indent=4))
        return
    else:
        with open(output_file, 'w') as f:
            json.dump(merged_settings, f, indent=4)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Merge system settings JSON files.")
    parser.add_argument("base_file", help="Path to the base system settings file.")
    parser.add_argument("override_file", help="Path to the override system settings file.")
    parser.add_argument("output_file", help="Path to save the merged output file. Use '-' for stdout.")

    args = parser.parse_args()

    merge_system_settings(args.base_file, args.override_file, args.output_file)
