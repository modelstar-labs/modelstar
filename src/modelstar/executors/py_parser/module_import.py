from dataclasses import dataclass
from gettext import find
from modelstar.executors.py_parser.implib_stdlib import py38
from modelstar.executors.py_parser.implib_snowflake import anaconda, src_alias
from modelstar.connectors.snowflake.modelstar import SNOWFLAKE_FILE_HANDLER_PATH
import os


@dataclass
class ModuleImport:
    '''Class for storing Imports.'''
    module: str
    submodule: str = None
    names: list = None

    import_type: str = None
    module_type: str = None
    abs_path: str = None
    rel_path: str = None

    def check_import(self):
        if self.module in src_alias:
            self.module = src_alias[self.module]['alias']
            
        if self.module in py38:
            self.module_type = 'py38_stdlib'
        elif self.module in anaconda:
            self.module_type = 'snowflake_imppkg'
        elif self.module == 'modelstar':
            self.module_type = 'modelstar'
            self.abs_path = os.path.dirname(SNOWFLAKE_FILE_HANDLER_PATH)
        else:
            self.module_type = 'local_imppkg'
            search_dir = os.path.join(os.getcwd(), 'functions')
            dirlist = os.listdir(search_dir)

            dirdict = {}
            for item in dirlist:
                item_name = item.split('.')[0]
                if item_name != '':
                    dirdict[item_name] = item
                else:
                    dirdict[item] = item

            # TODO Make custom error for these.
            if self.module not in dirdict:
                raise ValueError(f'Import module: `{self.module}` not found.')

            module_abs_path = os.path.abspath(
                os.path.join(search_dir, dirdict[self.module]))

            if not os.path.exists(module_abs_path):
                raise ValueError(f'Import module: `{self.module}` not found.')

            self.abs_path = module_abs_path
            self.rel_path = os.path.relpath(module_abs_path, search_dir)


def parse_import(node):
    module = []
    submodule = ''

    for alias in node.names:
        module_dots = alias.name.split('.')
        module = module_dots[0]
        submodule = alias.name
        yield ModuleImport(module=module, submodule=submodule, import_type='import')


def parse_importfrom(node):

    if node.module != None:
        module_dots = node.module.split('.')
        module = module_dots[0]
        submodule = node.module
        names = []
        for n in node.names:
            names.append(n.name)
    else:
        if len(node.names) == 1:
            module = node.names[0].name
            submodule = None
            names = None
        else:
            raise ValueError('Imports not valid.')

    return ModuleImport(module=module, submodule=submodule, names=names, import_type='import_from')
