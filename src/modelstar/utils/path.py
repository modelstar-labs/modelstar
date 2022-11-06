import os
from typing import List


def strip_file_namespace_pointer(file_namespace_pointer: str):

    file_namespace_pointer = file_namespace_pointer.strip()

    pointers = file_namespace_pointer.split(':')

    file_pointer = pointers[0]

    _, file_extension = os.path.splitext(pointers[0])

    # if file_extension == '':
    #     file_pointer = pointers[0] + '.py'
    # elif file_extension == '.py':
    #     file_pointer = pointers[0]
    # else:
    #     raise ValueError(
    #         'Provide a valid `<file_location>:<namepsace_pointer>`')

    if file_extension == '':
        raise ValueError(
            'Provide a valid `<file_location>:<namepsace_pointer>`')

    abs_file_path = check_file_path(file_pointer)

    namespace_pointer = pointers[1]

    file_name = os.path.basename(abs_file_path)

    file_folder_path = os.path.dirname(abs_file_path)

    return abs_file_path, file_folder_path, file_name, namespace_pointer


def check_file_path(file_path: str) -> str:
    file_path = file_path.strip()
    abs_file_path = os.path.abspath(file_path)

    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(
            f"Unable to locate {file_path} or it does not exist. Tip: provide an absolute path.")

    if not os.path.isfile(abs_file_path):
        raise ValueError(f'`{file_path}` is not a valid file.')

    return abs_file_path


def if_exists_else_create_file_folder(ff_path: str, ff_type: str):
    if not os.path.exists(ff_path):
        if ff_type == 'file':
            with open(ff_path, 'w') as file:
                pass
        if ff_type == 'folder':
            os.mkdir(ff_path)

    else:
        if ff_type == 'file':
            if not os.path.isfile(ff_path):
                raise ValueError(f'Existing File/Folder Conflict: {ff_path}')
        if ff_type == 'folder':
            if not os.path.isdir(ff_path):
                raise ValueError(f'Existing File/Folder Conflict: {ff_path}')