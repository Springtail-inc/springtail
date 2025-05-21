import os
import tarfile

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

def create_tar_gz(source_dir, output_path):
    """
    Compresses a folder into a .tar.gz file.
    """
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    print(f"Created archive: {output_path}")

def is_tar_gz_empty(tar_path):
    """
    Checks if the .tar.gz archive contains any files.
    Ignores directories.
    """
    with tarfile.open(tar_path, "r:gz") as tar:
        files = [member for member in tar.getmembers() if member.isfile()]
        return len(files) == 0

def extract_tar_gz(tar_path, extract_to):
    """
    Extracts a .tar.gz file to a target directory.
    """
    os.makedirs(extract_to, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tar:
        if is_tar_gz_empty(tar_path):
            print(f"[*] Tar file {tar_path} is empty")
            return False
        tar.extractall(path=extract_to)
    print(f"Extracted {tar_path} to {extract_to}")
    return True

def print_banner(message, pad=2, border='*'):
    lines = message.splitlines()
    width = max(len(line) for line in lines) + pad * 2
    horizontal_border = border * (width + 2)
    print(horizontal_border)
    for line in lines:
        print(f"{border} {line.center(width)} {border}")
    print(horizontal_border)
