from modelstar.executors.py_parser.module_function import ModuleFunction
from modelstar.connectors.snowflake.context_types import SnowflakeConfig, type_map_parameter, type_map_return
import os


def register_udf_from_file(config: SnowflakeConfig, file_path: str, function: ModuleFunction, imports: list, package_imports: list, version: str):

    stage = config.stage
    file_name = os.path.basename(file_path)

    import_list = [f"'{x}'" for x in imports]
    import_list.append(f"'@{stage}/{function.name}/{version}/{file_name}'")
    import_list_string = ', '.join(import_list)

    package_list = [f"'{x}'" for x in package_imports]
    package_list_string = ', '.join(package_list)

    sql_statements = []

    sql_statements.append(
        f'put file://{file_path} @{stage}/{function.name}/{version}')
    sql_statements.append(f"""create or replace function {function.name}({function.sql_param_list(type_mapper = type_map_parameter)})
returns {function.returns.sql_type(type_mapper = type_map_return)}
language python
runtime_version = '3.8'
packages = ({package_list_string})
handler = '{function.module_name}.{function.name}'
imports = ({import_list_string});""")

    return sql_statements
