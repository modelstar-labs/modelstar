import snowflake.connector
from modelstar.types.parsing import ModuleFunction
import os


def register_from_file(config, file_path: str, function: ModuleFunction):
    ctx = snowflake.connector.connect(
        user=config['username'],
        password=config['password'],
        account=config['account']
    )
    cs = ctx.cursor()
    try:
        database = config['database']
        schema = config['schema']
        stage = config['stage']
        file_name = os.path.basename(file_path)
        module_name = file_name.split('.')[0]

        cs.execute(f'use {database}.{schema}')
        cs.execute(f'put file://{file_path} @{stage}')
        cs.execute(f"""create or replace function {function.name}({function.sql_param_list()})
returns {function.returns.type}
language python
runtime_version = '3.8'
handler = '{module_name}.{function.name}'
imports = ('@{stage}/{file_name}');""")
        
    except Exception as e:
        print(e.args)
    finally:
        cs.close()
    ctx.close()


def sql_command_register():
    pass


def sql_command_put():
    pass


'''
TODO

Parse
- function file for the function name
- function file to get its dependencies and files
- function file to get the input types: if not just use VARIANT
- function file to get the output types: if not just use VARIANT


Check for: 
- database if it exists
- schema if it exists
- stage if it exists
- permission to write to it if it exists
- check if the function or the files with the names exists

https://github.com/snowflakedb/snowpark-python/blob/1ee21420b704e1ad595257b95365567fa864fddc/src/snowflake/snowpark/udf.py#L96
'''
