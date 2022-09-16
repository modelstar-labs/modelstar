from modelstar.executors.py_parser.module_function import ModuleFunction
import os


def register_udf_from_file(config, file_path: str, function: ModuleFunction, imports: list):
    database = config.database
    schema = config.schema
    stage = config.stage
    file_name = os.path.basename(file_path)

    import_list = [f"'{x}'" for x in imports]
    import_list.append(f"'@{stage}/{file_name}'")
    import_list_string = ', '.join(import_list)

    sql_statements = []

    sql_statements.append(f'use {database}.{schema}')
    sql_statements.append(f'put file://{file_path} @{stage}')
    sql_statements.append(f"""create or replace function {function.name}({function.sql_param_list()})
returns {function.returns.type}
language python
runtime_version = '3.8'
handler = '{function.module_name}.{function.name}'
imports = ({import_list_string});""")

    return sql_statements


def put_file_from_local(config, file_path: str):
    database = config.database
    schema = config.schema
    stage = config.stage

    sql_statements = []

    # TODO add threads to this to make this faster.
    sql_statements.append(f'use {database}.{schema}')
    sql_statements.append(f'put file://{file_path} @{stage}/func/')

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
