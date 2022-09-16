import os
from modelstar.executors.py_parser import parse_function_file
from modelstar.connectors.snowflake.context import SnowflakeContext, SnowflakeConfig, SnowflakeResponse
from modelstar.connectors.snowflake.modelstar import SNOWFLAKE_FILE_HANDLER_PATH
from modelstar.utils.zip import zip_local_imports, zip_modelstar_pkg


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
    version = 'v3'

    # TODO Add stage name here, or use default user stage
    # TODO do the above in the config context

    abs_file_path = check_function_path(file_name)
    function_register = parse_function_file(
        abs_file_path, file_name, function_name)

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)

        put_files_to_stage = []

        read_files_to_stage = [
            x.local_path for x in function_register.read_files]
        put_files_to_stage.extend(read_files_to_stage)

        local_module_zip_list = []
        snowflake_package_imports = []
        put_modelstar_pkg = False

        for imp in function_register.imports:
            imp.check_import()
            if imp.module_type == 'local_imppkg':
                local_module_zip_list.append(imp)
            elif imp.module_type == 'modelstar':
                put_modelstar_pkg = True
            elif imp.module_type == 'snowflake_imppkg':
                snowflake_package_imports.append(imp.module)

        local_imports_zip_to_stage = zip_local_imports(local_module_zip_list)
        put_files_to_stage.append(local_imports_zip_to_stage)

        if put_modelstar_pkg:
            modelstar_pkg_to_stage = zip_modelstar_pkg(
                registry_name=function_name, registry_version=version)
            put_files_to_stage.append(modelstar_pkg_to_stage)

        # Imports to be added to the function from the stage
        import_paths_from_stage = []

        # If a file load is present upload the file.
        for file_path in put_files_to_stage:
            response = snowflake_context.put_file(
                file_path=file_path, stage_path=f'{function_name}/{version}')
            import_paths_from_stage.append(response.info['file_stage_path'])

        # Register the function with the imports, packages and stage path
        response = snowflake_context.register_udf(
            file_path=abs_file_path, function=function_register.function, imports=import_paths_from_stage, package_imports=snowflake_package_imports, stage_path=f'{function_name}/{version}')
    else:
        raise ValueError(f'Failed to register function: {function_name}')

    if isinstance(response, SnowflakeResponse):
        return print(response.table.print())
    else:
        return response
