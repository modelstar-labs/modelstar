import os


def strip_function_file_pointer(function_file_pointer: str):

    function_file_pointer = function_file_pointer.strip()

    pointers = function_file_pointer.split(':')

    _, file_extension = os.path.splitext(pointers[0])

    if file_extension == '':
        file_pointer = pointers[0] + '.py'
    elif file_extension == '.py':
        file_pointer = pointers[0]
    else:
        raise ValueError('Provide a valid `<file_location>:<function_handler>`')

    if not os.path.exists(file_pointer):
        raise ValueError(f'File `{file_pointer}` does not exist.')

    if not os.path.isfile(file_pointer):
        raise ValueError(f'`{file_pointer}` not a valid file.')

    function_pointer = pointers[1]

    file_path = os.path.abspath(file_pointer)

    file_name = os.path.basename(file_pointer)

    file_folder_path = os.path.dirname(file_path)

    return file_path, file_folder_path, function_pointer, file_name
