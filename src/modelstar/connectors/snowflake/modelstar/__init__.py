import inspect
import os
import sys
import joblib
import pickle
from dataclasses import dataclass
from dataclasses import field
import random
import string
from datetime import datetime

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
    run_id: str = None
    call_name: str = field(default_factory=lambda: REGISTRY_NAME)
    call_version: str = field(default_factory=lambda: REGISTRY_VERSION)
    database: str = None
    schema: str = None
    stage: str = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    records: list = field(default_factory=lambda: [])

    def write_records(self):
        write_path = 'run_record/' + self.run_id + '.modelstar.joblib'
        record_object = {
            'call_name': self.call_name,
            'call_version': self.call_version,
            'run_id': self.run_id,
            'call_location': f'{self.database.upper()}/{self.schema.upper()}',
            'stage': self.stage,
            'run_timestamp': self.timestamp.strftime("%m/%d/%Y, %H:%M:%S"),
            'records': self.records
        }
        modelstar_write_path(local_path=write_path, write_object=record_object)


# https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-creating.html#reading-and-writing-files-with-a-udf-handler
MODELSTAR_PKG_SNOWFLAKE = os.path.abspath(__file__)

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
    # TODO: Migrate to https://book.pythontips.com/en/latest/context_managers.html

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


def modelstar_table2df(table_name: str):
    table_df = SNOWFLAKE_SESSION_STATE.session.table(table_name).to_pandas()

    return table_df


def modelstar_df2table(table_name: str):
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


def gen_random_id(length: int = 16):
    id = ''.join(random.choice(string.ascii_uppercase +
                 string.ascii_lowercase + string.digits) for _ in range(length))

    return id


def modelstar_record(record_type: str, content: str) -> None:

    SNOWFLAKE_SESSION_STATE.records.append(
        {'type': record_type, 'content': content})


def modelstar_register_pycaret_inference_udf(function_name: str, model_filename: str, handler_args: list) -> None:
    handler_arg_string =  ', '.join([f'{i["col_name"]} {i["col_type"]}' for i in handler_args])
    function_arg_string =  ', '.join([f'{i["col_name"]}' for i in handler_args])

    sql_statement = f"""create or replace function {function_name}({handler_arg_string})
returns VARIANT
language python
runtime_version=3.8
packages = ('numpy', 'markupsafe', 'threadpoolctl', 'scikit-learn', 'pandas', 'matplotlib', 'importlib_metadata', 'plotly', 'jinja2', 'snowflake-snowpark-python', 'ipython', 'cycler', 'tqdm', 'lightgbm', 'packaging', 'joblib', 'category_encoders', 'psutil', 'numba', 'scipy', 'requests')
imports = ('@{STAGE_NAME}/{REGISTRY_NAME}/{REGISTRY_VERSION}/modelstar.zip', '@{STAGE_NAME}/{REGISTRY_NAME}/{REGISTRY_VERSION}/pycaret.zip', '@{STAGE_NAME}/{REGISTRY_NAME}/{REGISTRY_VERSION}/{model_filename}')
handler='{function_name}'
as
$$
import joblib
import pandas as pd
import pycaret.classification as pcc
from modelstar import modelstar_read_path, get_kwargs

def {function_name}({function_arg_string}):
    _model = joblib.load(modelstar_read_path(local_path = '{model_filename}'))
    
    kwargs = get_kwargs()    
    data = pd.DataFrame.from_records([kwargs])
    
    prediction_table = pcc.predict_model(_model[0], data=data)
    prediction_value = prediction_table[['prediction_label']].iat[0, 0]

    if type(prediction_value) == str:
        return_value = str(prediction_value)
    else:
        return_value = prediction_value

    return return_value
$$;
"""
    
    sql_ =  SNOWFLAKE_SESSION_STATE.session.sql(sql_statement)
    sql_.collect()
