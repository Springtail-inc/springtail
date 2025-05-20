import os

def get_dir_and_param(data, param):
    """
    Recursively search for a dict that contains both 'dir' and the given param.

    Returns:
        Tuple (dir_value, param_value), or (None, None) if not found.
    """
    if isinstance(data, dict):
        if "dir" in data and param in data:
            return data["dir"], data[param]
        for value in data.values():
            result = get_dir_and_param(value, param)
            if result != (None, None):
                return result
    elif isinstance(data, list):
        for item in data:
            result = get_dir_and_param(item, param)
            if result != (None, None):
                return result
    return None, None

def get_file_path(run_config: dict, file_name: str, dir_name: str = None) -> str:
    """
    Get the absolute path to a file based on the run configuration.

    Args:
        run_config (dict): The run configuration
        file_name (str): The name of the file
        dir_name (str): The name of the directory (optional)

    Returns:
        str: The absolute path to the file
    """
    # Traverse the yaml and find the file name
    dir_path, file_path = get_dir_and_param(run_config, file_name)

    if dir_path is None or file_path is None:
        raise ValueError(f"File {file_name} not found in run configuration")

    if dir_name:
        dir_path = os.path.join(dir_name, dir_path)
    complete_file_path = os.path.join(dir_path, file_path)

    # Create the directory if it doesn't exist
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return complete_file_path
