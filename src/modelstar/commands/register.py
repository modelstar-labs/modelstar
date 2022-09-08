import os
from pprint import pprint
from modelstar.executors.py_parser import parse_module_nodes
from modelstar.connectors.snowflake.context import SnowflakeContext, SnowflakeConfig


def check_function_path(file_name: str):
    file_name = file_name.strip()
    # Clean target : check if a .py file or a path to a .py file.
    current_working_directory = os.getcwd()
    functions_folder_path = os.path.join(
        current_working_directory, 'functions')
    function_file_path = os.path.join(functions_folder_path, file_name)

    if not os.path.exists(function_file_path):
        raise ValueError(f"{file_name} does not exist")

    abs_file_path = os.path.abspath(function_file_path)

    return abs_file_path


def parse_function_file(abs_file_path: str, function_name: str, file_name: str):
    # Get the imports and function list.
    # send -> abs file path and get all the info about the functions and imports.
    parsed_nodes = parse_module_nodes(abs_file_path)

    functions_in_modules = []
    for func in parsed_nodes.functions:
        functions_in_modules.append(func.name)
        if function_name == func.name:
            func.check_typing()
            register_function = func

    for call in parsed_nodes.calls:
        call.check_paths()
        pprint(call)

    if function_name not in functions_in_modules:
        raise ValueError(
            f"Function `{function_name}` not present in {file_name}.")

    return register_function


def register_function(config, function_name: str, file_name: str):
    # TODO Add stage name here, or use default user stage

    abs_file_path = check_function_path(file_name)
    register_function = parse_function_file(
        abs_file_path, function_name, file_name)

    return register_function

    # if isinstance(config, SnowflakeConfig):
    #     snowflake_context = SnowflakeContext(config)
    #     response = snowflake_context.register_udf(
    #         file_path=abs_file_path, function=register_function)
    # else:
    #     raise ValueError(f'Failed to register function: {function_name}')

    # return response.print()
