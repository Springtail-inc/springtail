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

def get_file_path(run_config: dict, file_name: str, dir_name: str = None, use_dir: str = "base_dir", create_dir: bool = True) -> str:
    """
    Get the absolute path to a file based on the run configuration.

    Args:
        run_config (dict): The run configuration
        file_name (str): The name of the file
        dir_name (str): The name of the directory (optional)
        use_dir (str): The name of the directory to use (optional)

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

    if create_dir:
        os.makedirs(os.path.dirname(complete_file_path), exist_ok=True)

    return complete_file_path

def zip_folder(source_dir, zip_path):
    """
    Zip a folder.

    Args:
        source_dir (str): The source directory
        zip_path (str): The path to the zip file
    """
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

    Args:
        zip_path (str): The path to the zip file
        extract_to (str): The directory to extract to (optional)
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
    """
    Print a banner with the given message.

    Args:
        message (str): The message to print
        pad (int): The padding to use (optional)
        border (str): The border character to use (optional)
    """
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

def print_results(results):
    """
    Print the results in table format.
    Auto pads the header and content rows to the same width.

    Args:
        results (list): List of results
    """
    # Dynamically determine column widths based on content
    status_title = "STATUS"
    metric_title = "METRIC"
    percentage_title = "PERCENTAGE"
    description_title = "DESCRIPTION"

    status_width = max(len(status_title), max(len("GOOD"), len("BAD"), len("NORMAL")))
    metric_width = max(len(metric_title), max(len(res["label"]) for res in results))
    percent_width = max(len(percentage_title), len("100.00 %"))
    desc_width = max(len(description_title), max(len(res["description"]) for res in results))

    # Print header
    print("|" + "-" * (status_width + 4) + "+" + "-" * (metric_width + 4) + "+" + "-" * (percent_width + 4) + "+" + "-" * (desc_width + 4) + "|")
    print(f"| {status_title:^{status_width + 2}} | {metric_title:^{metric_width + 2}} | {percentage_title:^{percent_width + 2}} | {description_title:^{desc_width + 2}} |")
    print("|" + "-" * (status_width + 4) + "+" + "-" * (metric_width + 4) + "+" + "-" * (percent_width + 4) + "+" + "-" * (desc_width + 4) + "|")

    # Print rows
    for res in results:
        status = "GOOD" if res["improvement"] else "BAD" if res["improvement"] is False else "NORMAL"
        desc = res["description"]
        metric = res["label"]
        percentage = f"{res['percentage']:.2f}"

        print(f"| {status:<{status_width + 2}} | {metric:<{metric_width + 2}} | {percentage:>{percent_width + 2}} | {desc:<{desc_width + 2}} |")

    # Footer
    print("|" + "-" * (status_width + 4) + "+" + "-" * (metric_width + 4) + "+" + "-" * (percent_width + 4) + "+" + "-" * (desc_width + 4) + "|")
