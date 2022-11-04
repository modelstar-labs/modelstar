import os


def check_project_folder_structure():
    # os.walk to get files and dirs: https://www.tutorialspoint.com/python/os_walk.htm

    cwd = os.getcwd()
    cwd_list_set = set(os.listdir(cwd))

    # assert project_list_set.issubset(
    #     cwd_list_set), "Missing files, folders or not a modelstar project"
    # project_list_set = set(['modelstar.config.yaml', '.modelstar'])
    # assert 'modelstar.config.yaml' in cwd_list_set, "Missing `modelstrat.toml` configuration file."

    if 'modelstar.config.yaml' in cwd_list_set:
        config_file_path = os.path.join(cwd, 'modelstar.config.yaml')
        if not os.path.isfile(config_file_path):
            raise ValueError('The `modelstar.config.yaml` present in the current working directory is not a file. Please create one as a file and configure your warehouse in it. Check out https://docs.modelstar.io for more information.')
    else:
        raise ValueError(
            'Current working directory does not contain a `modelstar.config.yaml` file. Please create one and configure your warehouse in it. Check out https://docs.modelstar.io for more information.')

    dotms_path = os.path.join(cwd, '.modelstar')
    if '.modelstar' in cwd_list_set:
        if not os.path.isdir(dotms_path):
            raise ValueError(
                '`.modelstar` present in the current folder is a file. Please delete it as it conflicts with the Modelstar folder structure.')
    else:
        os.mkdir(dotms_path)

    # Check if the path contains the required files.
    dotmos_list_set = set(os.listdir(dotms_path))

    dotmos_tmp_path = os.path.join(dotms_path, '.tmp')
    if ('.tmp' in dotmos_list_set):
        if not os.path.isdir(dotmos_tmp_path):
            raise ValueError(
                '`.tmp` present in the current folder is a file. Please delete it as it conflicts with the Modelstar folder structure.')
    else:
        os.mkdir(dotmos_tmp_path)

    if 'session.config.yaml' not in dotmos_list_set:
        session_path = os.path.join(dotms_path, 'session.config.yaml')
        with open(session_path, 'w') as file:
            file.write("""# MODELSTAR INTERNAL FILES: SESSION
---""")
