import os
import zipfile
from typing import List
from modelstar.executors.py_parser.module_import import ModuleImport
from modelstar.connectors.snowflake.modelstar import SNOWFLAKE_FILE_HANDLER_PATH
from modelstar.utils.path import if_exists_else_create_file_folder


def zip_local_imports(zip_list: List[ModuleImport]):
    src_base = os.path.join(os.getcwd(), 'functions')

    src_dest = []
    for ff in zip_list:
        if os.path.isfile(ff.abs_path):
            src_dest.append(
                {'source': ff.abs_path, 'destination': os.path.relpath(ff.abs_path, src_base)})
        elif os.path.isdir(ff.abs_path):
            for path, _, files in os.walk(ff.abs_path):
                for name in files:
                    src_path = os.path.join(path, name)
                    dest_path = os.path.relpath(src_path, src_base)
                    src_dest.append(
                        {'source': src_path, 'destination': dest_path})
        else:
            raise ValueError('Unable to create local modules zip')

    zip_path = os.path.join(os.getcwd(), '.modelstar/.tmp')

    # If zip_path folder doesn't exist, then create it.
    if_exists_else_create_file_folder(ff_path=zip_path, ff_type='folder')

    zip_name = 'local_modules.zip'
    zip_file_path = os.path.join(zip_path, zip_name)

    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for ff in src_dest:
            zipf.write(ff['source'], ff['destination'])

    return zip_file_path


def zip_modelstar_pkg(registry_name: str, registry_version: str, stage_name: str):
    zip_name = 'modelstar.zip'
    zip_path = os.path.join(os.getcwd(), '.modelstar/.tmp')
    zip_file_path = os.path.join(zip_path, zip_name)

    src_dest = []

    modelstar_base_path = os.path.dirname(SNOWFLAKE_FILE_HANDLER_PATH)
    # check if the abs path is a dir
    # check if the path has a base name .

    if os.path.isdir(modelstar_base_path):
        modelstar_package_path = os.path.dirname(modelstar_base_path)
        for path, _, files in os.walk(modelstar_base_path):
            for name in files:
                _, ext = os.path.splitext(name)
                if ext not in ['.pyc']:
                    src_path = os.path.join(path, name)
                    dest_path = os.path.relpath(
                        src_path, modelstar_package_path)
                    src_dest.append(
                        {'source': src_path, 'destination': dest_path})
    else:
        raise ValueError('Error in building modelstar for your registry.')

    constants_str = f"""# This file contains the constants for the modelstar registry
PATH_SYSTEM = 'snowflake'
REGISTRY_NAME = '{registry_name}'
REGISTRY_VERSION = '{registry_version}'
STAGE_NAME = '{stage_name}'
"""

    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for ff in src_dest:
            zipf.write(ff['source'], ff['destination'])
        zipf.writestr('modelstar/constants.py', constants_str)

    return zip_file_path
