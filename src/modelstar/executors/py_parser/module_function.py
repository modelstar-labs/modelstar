import ast
from dataclasses import dataclass
from modelstar.executors.py_parser.parse_exceptions import UDFTypeError

# TODO Add support for decimal.Decimal and datetime.(date, time, datetime)
ACCEPTED_PYTHON_FUNCTION_TYPES = [
    'str', 'int', 'float', 'list', 'bytes', 'bool', 'dict', 'DataFrame']


@dataclass
class FunctionParameter:
    '''Class for storing Function Parameters.'''
    name: str
    type: str
    pos: int


@dataclass
class FunctionReturn:
    '''Class for storing Function Returns.'''
    type: str

    def sql_type(self, type_mapper: dict):
        return map_py_to_sql_type(self.type, type_mapper)

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

    def sql_param_list(self, type_mapper: dict) -> str:
        sql_sep = ', '
        param_list = [param.name + ' ' +
                      map_py_to_sql_type(param.type, type_mapper) for param in self.parameters]
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
    param_pos = 0
    for arg in node_args:
        if isinstance(arg.annotation, ast.Name):
            param_type = arg.annotation.id
        elif isinstance(arg.annotation, ast.Attribute):
            param_type = arg.annotation.attr
        else:
            param_type = None

        func_params.append(FunctionParameter(
            name=arg.arg, type=param_type, pos=param_pos))

        param_pos = param_pos + 1

    if node.returns is not None:
        if isinstance(node.returns, ast.Name):
            ret_type = node.returns.id
        else:
            ret_type = 'Unknown'
    else:
        ret_type = None

    func_returns = FunctionReturn(type=ret_type)

    return ModuleFunction(name=func_name, docstring=ast.get_docstring(node), parameters=func_params, returns=func_returns, file_name=file_name, module_name=module_name, line_no=func_lineno)


def map_py_to_sql_type(py_type: str, type_mapper: dict) -> str:

    if py_type in type_mapper:
        sql_type = type_mapper[py_type]
    else:
        raise ValueError(f'Un-recognized python type `{py_type}`')

    return sql_type
