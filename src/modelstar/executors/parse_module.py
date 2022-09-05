from pprint import pprint
import ast
import importlib
from modelstar.types.parsing import ModuleFunction, ModuleImport, FunctionParameter, FunctionReturn 

# TODO implement this as a ast.NodeVisitor


def parse_import(node):
    module = []

    for n in node.names:
        return ModuleImport(module, n.name.split('.'), n.asname)


def parse_importfrom(node):
    module = node.module.split('.')

    for n in node.names:
        return ModuleImport(module, n.name.split('.'), n.asname)


def parse_function(node):
    # TODO reference the line no of the function and name in errors.
    # TODO create ModelStar Error Types
    func_lineno = node.lineno
    func_name = node.name
    entry = {"docstring": ast.get_docstring(node), "name": node.name}

    # ['args', 'defaults', 'kw_defaults', 'kwarg', 'kwonlyargs', 'posonlyargs', 'vararg']
    node_args = node.args.posonlyargs
    node_args.extend(node.args.args)
    node_args.extend(node.args.kwonlyargs)

    func_params = []
    for arg in node_args:
        if hasattr(arg.annotation, 'id'):
            param_type = arg.annotation.id
            if param_type in ['str', 'int', 'float', 'list', 'bytes', 'bool', 'dict']:
                func_params.append(FunctionParameter(
                    name=arg.arg, type=param_type))
            else:
                # TODO Add support for decimal.Decimal and datetime.(date, time, datetime)
                raise ValueError(
                    'Use only the base types of str, int, float, bool, list, dict')
        else:
            raise ValueError(
                'Please specify types for the function arguments.')

    # SQL returns only one type.
    # https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-designing.html#sql-python-data-type-mappings-for-parameters-and-return-types
    if node.returns is not None:
        if isinstance(node.returns, ast.Name):
            ret_type = node.returns.id
            if ret_type in ['str', 'int', 'float', 'list', 'bytes', 'bool', 'dict']:
                func_returns = FunctionReturn(type=ret_type)
            else:
                # TODO Add support for decimal.Decimal and datetime.(date, time, datetime)
                raise ValueError(
                    'Use only the base types of str, int, float, bool, list, dict')
        else:
            # Raise ModelStarSyntax error.
            raise ValueError(
                'Use only the base types of str, int, float, bool, list, dict')
        entry['returns'] = func_returns
    else:
        # TODO check if this is necessary.
        raise ValueError(
            'A UDF to Snowflake must always return a value.')

    return ModuleFunction(name=func_name, docstring=ast.get_docstring(node), parameters=func_params, returns=func_returns)


def parse_nodes(module):
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef):
            yield parse_function(node)
        elif isinstance(node, ast.Import):
            yield parse_import(node)
        elif isinstance(node, ast.ImportFrom):
            yield parse_importfrom(node)
        else:
            continue


def get_info(path: str):
    module_functions = []
    module_imports = []
    with open(path, "r") as source:
        module = ast.parse(source.read(), path)        
        for node_info in parse_nodes(module):
            if isinstance(node_info, ModuleFunction):
                module_functions.append(node_info)
            if isinstance(node_info, ModuleImport):
                module_imports.append(node_info)

    return module_functions


    # 'contents', 'get_filename', 'get_resource_reader', 'get_source', 'is_package', 'is_resource',  'name', 'path', 'resource_path'
    # pprint(importlib.machinery.PathFinder().find_module("proj.pkg1").get_filename())
    # pprint(importlib.machinery.PathFinder().find_module("proj").resource_path())

    # pprint(importlib.util.find_spec('proj.m1'))
