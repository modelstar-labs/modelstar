import os
import sys

try:
    from .constants import PATH_SYSTEM
except:
    PATH_SYSTEM = 'local'

try:
    from .constants import REGISTRY_NAME, REGISTRY_VERSION
except:
    REGISTRY_NAME = None
    REGISTRY_VERSION = None


# https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-creating.html#reading-and-writing-files-with-a-udf-handler

SNOWFLAKE_FILE_HANDLER_PATH = os.path.abspath(__file__)

SNOWFLAKE_IMPORT_DIRECTORY_NAME = "snowflake_import_directory"


def modelstar_read_path(local_path: str = None, snowflake_path: str = None) -> str:
    if PATH_SYSTEM == 'local':
        path = local_path
    elif PATH_SYSTEM == 'snowflake':
        path = stage_to_dir(local_path)
    else:
        path = local_path
    return path


def modelstar_write_path(local_path: str):
    return local_path


def stage_to_dir(local_path: str) -> str:
    # local_path is '/connectors/snowflake/modelstar/constants.py'
    file_name = os.path.basename(local_path)
    import_dir = sys._xoptions[SNOWFLAKE_IMPORT_DIRECTORY_NAME]

    path_strings = []

    # if REGISTRY_NAME is not None:
    #     path_strings.append(REGISTRY_NAME)
    # if REGISTRY_VERSION is not None:
    #     path_strings.append(REGISTRY_VERSION)
    path_strings.append(file_name)

    file_in_stage = '/'.join(path_strings)
    final_path = os.path.join(import_dir, file_in_stage)

    return final_path
