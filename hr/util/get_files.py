def get_files(directory):
    import os
    """
    creates a dictionary of files in the directory of each directory in the argument dir
    """
    files = {}
    for dir in os.listdir(directory):
        dir_path = os.path.join(directory, dir)
        if os.path.isdir(dir_path):
            files[dir] = []
            for file in os.listdir(dir_path):
                if not file.startswith('.'):
                    # Check if the item is a file
                    file_path = os.path.join(dir_path, file)
                    if os.path.isfile(file_path):
                        files[dir].append(file_path)
    return files
