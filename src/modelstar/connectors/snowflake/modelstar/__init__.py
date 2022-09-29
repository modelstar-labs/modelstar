import inspect
import os
import sys
import joblib
import pickle
from dataclasses import dataclass

try:
    from .constants import PATH_SYSTEM
except:
    PATH_SYSTEM = 'local'

try:
    from .constants import REGISTRY_NAME, REGISTRY_VERSION, STAGE_NAME
except:
    REGISTRY_NAME = None
    REGISTRY_VERSION = None
    STAGE_NAME = None


@dataclass
class SnowflakeSessionState():
    session = None


# https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-creating.html#reading-and-writing-files-with-a-udf-handler
SNOWFLAKE_FILE_HANDLER_PATH = os.path.abspath(__file__)

SNOWFLAKE_IMPORT_DIRECTORY_NAME = "snowflake_import_directory"

SNOWFLAKE_SESSION_STATE = SnowflakeSessionState()


def modelstar_read_path(local_path: str = None, snowflake_path: str = None) -> str:
    if PATH_SYSTEM == 'local':
        path = local_path
    elif PATH_SYSTEM == 'snowflake':
        path = stage_to_dir(local_path)
    else:
        path = local_path
    return path


def modelstar_write_path(local_path: str, write_object):

    file_name = os.path.basename(local_path)
    _, ext = os.path.splitext(file_name)

    if PATH_SYSTEM == 'local':
        write_object_file_path = local_path
    elif PATH_SYSTEM == 'snowflake':
        stage = "@test"
        write_object_output_dir = '/tmp'
        write_object_file_path = os.path.join(
            write_object_output_dir, file_name)
    else:
        pass

    if ext == '.joblib':
        joblib.dump(write_object, write_object_file_path)
    elif ext == '.pkl':
        pickle.dump(write_object, write_object_file_path)
    else:
        if ext in ['.txt']:
            pass
        else:
            raise ValueError('Unspported file format.')

    if PATH_SYSTEM == 'local':
        return local_path
    elif PATH_SYSTEM == 'snowflake':
        write_stage_path = f'@{STAGE_NAME}/{REGISTRY_NAME}/{REGISTRY_VERSION}'
        SNOWFLAKE_SESSION_STATE.session.file.put(
            write_object_file_path, write_stage_path, overwrite=True)
        return f"{write_stage_path}/{file_name}"
    else:
        pass


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


def modelstar_table_df(table_name: str):
    table_df = SNOWFLAKE_SESSION_STATE.session.table(table_name).to_pandas()

    return table_df


def get_kwargs():
    frame = inspect.currentframe().f_back
    keys, _, _, values = inspect.getargvalues(frame)
    kwargs = {}
    for key in keys:
        if key != 'self':
            kwargs[key] = values[key]
    return kwargs
