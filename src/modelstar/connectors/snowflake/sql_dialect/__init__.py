from modelstar.executors.table import TableInfo
from modelstar.connectors.snowflake.context_types import SnowflakeConfig, FileFormat
from modelstar.connectors.snowflake.sql_dialect.register_sproc import register_procedure_from_file
from modelstar.connectors.snowflake.sql_dialect.register_udf import register_udf_from_file


def session_use(config: SnowflakeConfig):
    database = config.database
    schema = config.schema
    stage = config.stage

    sql_statements = []
    sql_statements.append(f'USE {database}.{schema};')

    return sql_statements


def put_file_from_local(config: SnowflakeConfig, file_path: str, stage_path: str = None):

    # TODO add threads to this to make this faster.
    sql_statements = []
    if stage_path is not None:
        sql_statements.append(
            f'PUT file://{file_path} @{config.stage}/{stage_path} OVERWRITE = TRUE')
    else:
        sql_statements.append(
            f'PUT file://{file_path} @{config.stage} OVERWRITE = TRUE')

    return sql_statements


def get_file_from_stage(config: SnowflakeConfig, local_path: str, stage_path: str = None, name_pattern: str = None):

    # TODO add threads to this to make this faster.
    sql_statements = []

    if stage_path is not None:
        stmt = f'GET @{config.stage}/{stage_path} file://{local_path}'
    else:
        stmt = f'GET @{config.stage} file://{local_path}'

    if name_pattern is not None:
        # .*\.modelstar.joblib.*
        stmt = stmt + f" PATTERN='{name_pattern}'"

    sql_statements.append(stmt)

    return sql_statements


def clear_function_stage_files(config: SnowflakeConfig, function_name: str, version: str):

    sql_statements = []
    sql_statements.append(f'REMOVE @{config.stage}/{function_name}/{version}')

    return sql_statements


def create_table(config: SnowflakeConfig, table_info: TableInfo):

    sql_statements = []
    sql_statements.append(f'CREATE OR REPLACE TABLE {table_info.name} (')
    for idx, column in enumerate(table_info.columns):
        if idx == (len(table_info.columns)-1):
            sql_statements.append(f'{column.name} {column.snow_type}')
        else:
            sql_statements.append(f'{column.name} {column.snow_type},')

    sql_statements.append(f');')

    return [' '.join(sql_statements)]


def create_file_format(config: SnowflakeConfig, file_format: FileFormat):

    sql_statements = []
    sql_statements.append(
        f"CREATE OR REPLACE FILE FORMAT {file_format.format_name} type = '{file_format.format_type}' field_delimiter = '{file_format.delimiter}' SKIP_HEADER = {file_format.skip_header};")

    return sql_statements


def copy_file_into_table(config: SnowflakeConfig, table_info: TableInfo, file_stage_path: str, file_format: FileFormat):

    sql_statements = []
    sql_statements.append(
        f'COPY INTO {table_info.name} from {file_stage_path} file_format={file_format.format_name};')

    return sql_statements


'''
TODO

Parse
- function file to get its dependencies and files

Check for: 
- database if it exists
- schema if it exists
- stage if it exists
- permission to write to it if it exists
- check if the function or the files with the names exists

https://github.com/snowflakedb/snowpark-python/blob/1ee21420b704e1ad595257b95365567fa864fddc/src/snowflake/snowpark/udf.py#L96
'''
