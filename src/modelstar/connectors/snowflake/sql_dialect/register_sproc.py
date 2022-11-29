from modelstar.executors.py_parser.module_function import ModuleFunction
from modelstar.connectors.snowflake.context_types import SnowflakeConfig, type_map_parameter, type_map_return
import os


def register_procedure_from_file(config: SnowflakeConfig, file_path: str, function: ModuleFunction, imports: list, package_imports: list, version: str):

    stage = config.stage
    file_name = os.path.basename(file_path)

    import_list = [f"'{x}'" for x in imports]
    import_list.append(f"'@{stage}/{function.name}/{version}/{file_name}'")
    import_list_string = ', '.join(import_list)

    package_imports.append('pandas')
    package_imports.append('numpy')
    package_list = [f"'{x}'" for x in set(package_imports)]
    package_list_string = ', '.join(package_list)

    sql_statements = []

    table_2_df = []
    table_2_df_param_list = []
    for param in function.parameters:
        if param.type == 'DataFrame':
            table_2_df.append(str(param.pos))
            table_2_df_param_list.append(f"'{param.name}'")

    param_list_string = ', '.join(
        [param.name for param in function.parameters])
    table_2_df_param_list_string = ', '.join(table_2_df_param_list)

    sql_statements.append(
        f'put file://{file_path} @{stage}/{function.name}/{version}')
    sql_statements.append(f"""create or replace procedure {function.name}({function.sql_param_list(type_mapper = type_map_parameter)})
    returns {function.returns.sql_type(type_mapper = type_map_return)}
    language python
    runtime_version = '3.8'
    packages = ({package_list_string})
    imports = ({import_list_string})
    handler = 'procedure_handler'
AS
$$
from {function.module_name} import {function.name}
from modelstar import SNOWFLAKE_SESSION_STATE, get_kwargs, modelstar_table2df, modelstar_df2table, gen_random_id, modelstar_write_path
from snowflake.snowpark.session import Session
import pandas as pd
import numpy as np
from uuid import uuid4

SNOWFLAKE_SESSION_STATE.run_id = gen_random_id()
SNOWFLAKE_SESSION_STATE.database = '{config.database}'
SNOWFLAKE_SESSION_STATE.schema = '{config.schema}'
SNOWFLAKE_SESSION_STATE.stage = '{config.stage}'
# SNOWFLAKE_SESSION_STATE.call_name = '{function.name}'
# SNOWFLAKE_SESSION_STATE.call_version = '{version}'


def procedure_handler(session: Session, {param_list_string}):
    SNOWFLAKE_SESSION_STATE.session = session
    
    kwargs = get_kwargs()

    arg_vals = []
    for param_name, param_val in kwargs.items():
        if not isinstance(param_val, Session):
            if param_name in [{table_2_df_param_list_string}]:
                arg_vals.append(modelstar_table2df(param_val))
            else:
                arg_vals.append(param_val)
    
    result = {function.name}(*arg_vals)    

    SNOWFLAKE_SESSION_STATE.write_records()

    if isinstance(result, pd.DataFrame):
        result.columns = result.columns.str.upper()
        result_dtypes = result.dtypes.to_dict()

        date_time_cols = []
        all_cols = []
        for col, col_type in result_dtypes.items():
            all_cols.append(col)
            if col_type == np.dtype('datetime64[ns]'):
                date_time_cols.append(col)
                result[col] = result[col].dt.strftime('%Y-%m-%d %H:%M:%S')    

        col_transform = []
        for col in all_cols:
            if col in date_time_cols:
                col_transform.append(f'TO_TIMESTAMP_NTZ({{col}}) as {{col}}')
            else:
                col_transform.append(col)
        
        cols_transform_string = ', '.join(col_transform)

        result_table_name = 'result_{function.name}'.upper()
        session.write_pandas(result, result_table_name, auto_create_table=True, overwrite=True)

        session_sql = session.sql(f"CREATE or REPLACE TABLE {{result_table_name}} as SELECT {{cols_transform_string}} FROM {{result_table_name}}")
        session_sql.collect()

        return_result = {{ 'return_table': result_table_name, 'run_id' : SNOWFLAKE_SESSION_STATE.run_id }}
    elif type(result) == dict:
        return_result = result
        return_result['run_id'] = SNOWFLAKE_SESSION_STATE.run_id 
    else:
        return_result = {{ 'return_result': result, 'run_id' : SNOWFLAKE_SESSION_STATE.run_id }}

    return return_result
$$;""")

    return sql_statements
