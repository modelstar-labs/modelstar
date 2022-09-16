import os
import sys

try:
    from .constants import REGISTRY_NAME, REGISTRY_VERSION
except:
    REGISTRY_NAME = None
    REGISTRY_VERSION = None


# https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-creating.html#reading-and-writing-files-with-a-udf-handler

SNOWFLAKE_FILE_HANDLER_PATH = os.path.abspath(__file__)

PATH_SYSTEM = 'snowflake'

SNOWFLAKE_IMPORT_DIRECTORY_NAME = "snowflake_import_directory"


def modelstar_read_path(local_path: str = None, snowflake_path: str = None, test_system: str = None) -> str:
    if test_system == None:
        if PATH_SYSTEM == 'local':
            path = local_path
        elif PATH_SYSTEM == 'snowflake':
            path = stage_to_dir(snowflake_path)
        else:
            path = local_path
    else:
        if test_system == 'local':
            path = local_path
        elif test_system == 'snowflake':
            path = stage_to_dir(snowflake_path)
        else:
            path = local_path

    return path


def modelstar_write_path(file_path: str):
    return file_path


def stage_to_dir(snowflake_path: str) -> str:
    # snowflake_path is '@my_stage/file.txt'
    import_dir = sys._xoptions[SNOWFLAKE_IMPORT_DIRECTORY_NAME]

    path_strings = snowflake_path.split('/')
    stage_name = path_strings.pop(0)

    if not stage_name.startswith('@'):
        raise ValueError('Incorrect snowflake_path string.')

    file_in_stage = '/'.join(path_strings)
    final_path = os.path.join(import_dir, file_in_stage)

    return final_path
