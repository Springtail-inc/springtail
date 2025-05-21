import os
import zipfile

def get_dir_and_param(run_config: dict, param: str):
    """
    Recursively search for a dict that contains both 'dir' and the given param.

    Returns:
        Tuple (dir_value, param_value), or (None, None) if not found.
    """
    if isinstance(run_config, dict):
        if "dir" in run_config and param in run_config:
            return run_config["dir"], run_config[param]
        for value in run_config.values():
            result = get_dir_and_param(value, param)
            if result != (None, None):
                return result
    elif isinstance(run_config, list):
        for item in run_config:
            result = get_dir_and_param(item, param)
            if result != (None, None):
                return result
    return None, None

def get_file_path(run_config: dict, file_name: str, dir_name: str = None, use_dir: str = "base_dir") -> str:
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

    base_dir = run_config.get("file_configuration", {}).get(use_dir, "")
    if dir_path is None or file_path is None:
        raise ValueError(f"File {file_name} not found in run configuration")

    if dir_name:
        dir_path = os.path.join(base_dir, dir_name, dir_path)
    else:
        dir_path = os.path.join(base_dir, dir_path)
    complete_file_path = os.path.join(dir_path, file_path)

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(complete_file_path), exist_ok=True)

    return complete_file_path

def zip_folder(source_dir, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, start=source_dir)
                zf.write(full_path, arcname=rel_path)

def unzip_file(zip_path, extract_to = None):
    """
    Extracts a zip file to the given directory.
    Creates the directory if it does not exist.
    """
    if extract_to is None:
        extract_to = os.path.dirname(zip_path)
    print(f'[>] Extracting {zip_path} to {extract_to}')
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        if not zip_ref.infolist():
            print(f"[!] Zip file {zip_path} is empty")
            return False
        zip_ref.extractall(extract_to)
    return True

def print_banner(message, pad=1, border='*'):
    lines = message.splitlines()
    max_line_len = max(len(line) for line in lines)
    width = max_line_len + (pad * 2)
    horizontal_border = border * (width + 2)

    print(horizontal_border)
    for line in lines:
        # Center the line within the padded area
        padded_line = line.center(width)
        print(f"{border}{padded_line}{border}")
    print(horizontal_border)
