from modelstar.executors.py_parser import parse_function_file
from modelstar.connectors.snowflake.context import SnowflakeContext
from modelstar.connectors.snowflake.modelstar import SNOWFLAKE_FILE_HANDLER_PATH
from modelstar.utils.zip import zip_local_imports, zip_modelstar_pkg
from modelstar.connectors.snowflake.context_types import SnowflakeConfig, SnowflakeResponse


def register_function_from_file(config, function_name: str, file_name: str, file_path: str, version: str = 'V1'):

    # Register the function with the imports, packages and stage path
    function_register = parse_function_file(
        file_path, file_name, function_name)

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)

        snowflake_context.clear_existing_function_version(
            function=function_register.function, version=version)

        put_files_to_stage = []

        read_files_to_stage = [
            x.local_path for x in function_register.read_files]
        put_files_to_stage.extend(read_files_to_stage)

        local_module_zip_list = []
        snowflake_package_imports = ['joblib']
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
                registry_name=function_name, registry_version=version, stage_name=config.stage)
            put_files_to_stage.append(modelstar_pkg_to_stage)

        # Imports to be added to the function from the stage
        import_paths_from_stage = []

        # If a file load is present upload the file.
        # TODO: Make this as multi file import
        for local_file_path in put_files_to_stage:
            response = snowflake_context.put_file(
                file_path=local_file_path, stage_path=f'{function_name}/{version}')
            import_paths_from_stage.append(response.info['file_stage_path'])

        response = snowflake_context.register_udf(
            file_path=file_path, function=function_register.function, imports=import_paths_from_stage, package_imports=snowflake_package_imports, version=version)
    else:
        raise ValueError(f'Failed to register function: {function_name}')

    return response


def register_procedure_from_file(config, function_name: str, file_name: str, file_path: str, version: str = 'V1'):

    # Register the function with the imports, packages and stage path
    function_register = parse_function_file(
        file_path, file_name, function_name)

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)

        snowflake_context.clear_existing_function_version(
            function=function_register.function, version=version)

        put_files_to_stage = []

        read_files_to_stage = [
            x.local_path for x in function_register.read_files]
        put_files_to_stage.extend(read_files_to_stage)

        local_module_zip_list = []
        snowflake_package_imports = [
            'snowflake-snowpark-python', 'pandas', 'joblib']
        put_modelstar_pkg = True

        for imp in function_register.imports:
            imp.check_import()
            if imp.module_type == 'local_imppkg':
                local_module_zip_list.append(imp)
            elif imp.module_type == 'snowflake_imppkg':
                snowflake_package_imports.append(imp.module)

        local_imports_zip_to_stage = zip_local_imports(local_module_zip_list)
        put_files_to_stage.append(local_imports_zip_to_stage)

        if put_modelstar_pkg:
            modelstar_pkg_to_stage = zip_modelstar_pkg(
                registry_name=function_name, registry_version=version, stage_name=config.stage)
            put_files_to_stage.append(modelstar_pkg_to_stage)

        # Imports to be added to the function from the stage
        import_paths_from_stage = []

        # If a file load is present upload the file.
        for local_file_path in put_files_to_stage:
            response = snowflake_context.put_file(
                file_path=local_file_path, stage_path=f'{function_name}/{version}')
            import_paths_from_stage.append(response.info['file_stage_path'])

        response = snowflake_context.register_procedure(
            file_path=file_path, function=function_register.function, imports=import_paths_from_stage, package_imports=snowflake_package_imports, version=version)

    else:
        raise ValueError(f'Failed to register function: {function_name}')

    return response
