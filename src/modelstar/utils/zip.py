from genericpath import isdir
from typing import List
from modelstar.executors.py_parser.module_import import ModuleImport
import os
import zipfile


def zip_local_imports(zip_list: List[ModuleImport], ):
    src_base = os.path.join(os.getcwd(), 'functions')

    src_dest = []
    for ff in zip_list:
        if os.path.isfile(ff.abs_path):
            if ff.module_type == 'modelstar':
                src_dest.append(
                    {'source': ff.abs_path, 'destination': 'modelstar.py'})
            else:
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
    if not os.path.isdir(zip_path):
        os.makedirs(zip_path)

    zip_name = 'local_modules.zip'
    zip_file_path = os.path.join(zip_path, zip_name)

    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for ff in src_dest:
            zipf.write(ff['source'], ff['destination'])

    return zip_file_path
