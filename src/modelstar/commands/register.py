import os
from modelstar.executors.py_parser import parse_function_file
from modelstar.connectors.snowflake.context import SnowflakeContext, SnowflakeConfig
from modelstar.connectors.snowflake.modelstar import SNOWFLAKE_FILE_HANDLER_PATH


def check_function_path(file_name: str):
    # TODO mode all path check functions to a path utils modules
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


def register_function_from_file(config, function_name: str, file_name: str):
    # TODO Add stage name here, or use default user stage

    abs_file_path = check_function_path(file_name)
    function_register = parse_function_file(
        abs_file_path, file_name, function_name)

    return function_register

    # if isinstance(config, SnowflakeConfig):
    #     snowflake_context = SnowflakeContext(config)

    #     # Imports to be added from the stage
    #     import_paths_from_stage = []

    #     # If a file load is present upload the file.
    #     for read_file in function_register.read_files:
    #         response = snowflake_context.put_file(
    #             file_path=read_file.local_path)
    #         import_file_name = os.path.basename(read_file.local_path)
    #         import_paths_from_stage.append(
    #             f'@{config.stage}/{import_file_name}')

    #     if len(function_register.read_files) > 0:
    #         response = snowflake_context.put_file(
    #             file_path=SNOWFLAKE_FILE_HANDLER_PATH)
    #         import_file_name = os.path.basename(SNOWFLAKE_FILE_HANDLER_PATH)
    #         import_paths_from_stage.append(
    #             f'@{config.stage}/{import_file_name}')

    #     response = snowflake_context.register_udf(
    #         file_path=abs_file_path, function=function_register.function, imports=import_paths_from_stage)
    # else:
    #     raise ValueError(f'Failed to register function: {function_name}')

    # return response
