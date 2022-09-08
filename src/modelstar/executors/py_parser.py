import ast
import os
from dataclasses import dataclass
from typing import List

# TODO Add support for decimal.Decimal and datetime.(date, time, datetime)
ACCEPTED_PYTHON_FUNCTION_TYPES = [
    'str', 'int', 'float', 'list', 'bytes', 'bool', 'dict']


class UDFTypeError(Exception):
    """Exception raised for UDF type declaration errors.

    Attributes:
        file_name     : Name of the file where the function is located.
        function_name : Name of the function.
        line_no       : Line number of the function in the file. 
        type          : Type of error.
    """

    def __init__(self, file_name: str, function_name: str, line_no: int, type: str):
        self.file_name = file_name
        self.function_name = function_name
        self.line_no = line_no
        self.type = type
        self.message = f'Typing error for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        super().__init__(self.message)

    def __str__(self):
        if self.type == 'missing':
            message = f'Types for function argument missing for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        elif self.type == 'not-base':
            message = f'Use only Python base types for UDF functions. Error for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        else:
            message = f'Typing error for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        return message


@dataclass
class ModuleImport:
    '''Class for storing Imports.'''
    module: list
    name: list
    alias: str = None


def parse_import(node):
    module = []

    for n in node.names:
        return ModuleImport(module, n.name.split('.'), n.asname)


def parse_importfrom(node):
    module = node.module.split('.')

    for n in node.names:
        return ModuleImport(module, n.name.split('.'), n.asname)


@dataclass
class FunctionParameter:
    '''Class for storing Function Parameters.'''
    name: str
    type: str


@dataclass
class FunctionReturn:
    '''Class for storing Function Returns.'''
    type: str

    # SQL returns only one type.
    # https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-designing.html#sql-python-data-type-mappings-for-parameters-and-return-types


@dataclass
class ModuleFunction:
    name: str
    docstring: str
    parameters: 'list[FunctionParameter]'
    returns: 'FunctionReturn'
    file_name: str
    module_name: str
    line_no: int

    def sql_param_list(self) -> str:
        sql_sep = ', '
        param_list = [param.name + ' ' +
                      param.type for param in self.parameters]
        return sql_sep.join(param_list)

    def check_typing(self) -> None:
        for _parameter in self.parameters:
            if _parameter.type is None:
                raise UDFTypeError(
                    function_name=self.name, file_name=self.file_name, line_no=self.line_no, type='missing')
            elif _parameter.type not in ACCEPTED_PYTHON_FUNCTION_TYPES:
                raise UDFTypeError(
                    function_name=self.name, file_name=self.file_name, line_no=self.line_no, type='not-base')
            else:
                pass

        if self.returns.type is None:
            raise UDFTypeError(
                function_name=self.name, file_name=self.file_name, line_no=self.line_no, type='missing')
        elif self.returns.type not in ACCEPTED_PYTHON_FUNCTION_TYPES:
            raise UDFTypeError(
                function_name=self.name, file_name=self.file_name, line_no=self.line_no, type='not-base')
        else:
            pass


def parse_function(node, file_name, module_name):
    func_lineno = node.lineno
    func_name = node.name

    node_args = node.args.posonlyargs
    node_args.extend(node.args.args)
    node_args.extend(node.args.kwonlyargs)

    func_params = []
    for arg in node_args:

        if hasattr(arg.annotation, 'id'):
            param_type = arg.annotation.id
        else:
            param_type = None

        func_params.append(FunctionParameter(name=arg.arg, type=param_type))

    if node.returns is not None:
        if isinstance(node.returns, ast.Name):
            ret_type = node.returns.id
        else:
            ret_type = 'Unknown'
    else:
        ret_type = None

    func_returns = FunctionReturn(type=ret_type)

    return ModuleFunction(name=func_name, docstring=ast.get_docstring(node), parameters=func_params, returns=func_returns, file_name=file_name, module_name=module_name, line_no=func_lineno)


@dataclass
class ModelstarCall:
    name: str
    local_path: str = None
    snowflake_path: str = None

    def check_paths(self):
        if self.local_path == None:
            raise ValueError(
                'FilePathError: `local_path` parameter value missing.')
        if self.snowflake_path == None:
            raise ValueError(
                'FilePathError: `snowflake_path` parameter value missing.')


def parse_modelstar_call(node):
    call_name = node.func.id

    # Make sure the args are none:
    # TODO: Raise Error when args are entered instead of keywords.
    # pprint([ast.dump(arg) for arg in node.args])

    local_path = None
    snowflake_path = None

    for kw in node.keywords:
        if isinstance(kw.value, ast.Constant):
            path = kw.value.value
        else:
            raise ValueError(
                'Only constants allowed. Enter the path as a string.')

        if kw.arg == 'local_path':
            local_path = path

        if kw.arg == 'snowflake_path':
            snowflake_path = path

    return ModelstarCall(name=call_name, local_path=local_path, snowflake_path=snowflake_path)


class ModuleParser(ast.NodeVisitor):
    def __init__(self, file_name: str, module_name: str):
        self.file_name: str = file_name
        self.module_name: str = module_name
        self.imports: List[ModuleImport] = []
        self.functions: List[ModuleFunction] = []
        self.calls: List[ModelstarCall] = []

    def visit_Import(self, node):
        parsed_info = parse_import(node)
        self.imports.append(parsed_info)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        parsed_info = parse_importfrom(node)
        self.imports.append(parsed_info)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        parsed_info = parse_function(node, self.file_name, self.module_name)
        self.functions.append(parsed_info)
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            if node.func.id in ['modelstar_read_path', 'modelstar_write_path']:
                parsed_info = parse_modelstar_call(node)
                self.calls.append(parsed_info)
        self.generic_visit(node)


def parse_module_nodes(file_path: str) -> ModuleParser:

    file_name = os.path.basename(file_path)
    module_name, ext_name = file_name.split('.')

    assert ext_name == 'py', 'UDFs can only be registers from .py files.'

    visitor = ModuleParser(file_name, module_name)
    with open(file_path, "r") as source:
        module = ast.parse(source.read(), file_path)
        visitor.visit(module)

    return visitor
