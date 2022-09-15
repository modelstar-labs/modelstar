import ast
from dataclasses import dataclass
from modelstar.executors.py_parser.parse_exceptions import UDFTypeError

# TODO Add support for decimal.Decimal and datetime.(date, time, datetime)
ACCEPTED_PYTHON_FUNCTION_TYPES = [
    'str', 'int', 'float', 'list', 'bytes', 'bool', 'dict']


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
