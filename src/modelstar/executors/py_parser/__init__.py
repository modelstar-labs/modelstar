import ast
from modelstar.executors.py_parser.node_visitor import ModuleNodeVisitor, FunctionRegister


def parse_function_file(file_path: str, file_name: str, function_name: str,) -> FunctionRegister:

    module_name, ext_name = file_name.split('.')

    assert ext_name == 'py', 'UDFs can only be registered from .py files.'

    visitor = ModuleNodeVisitor(file_name, module_name, function_name)
    with open(file_path, "r") as source:
        module = ast.parse(source.read(), file_path)
        visitor.visit(module)
        function_register = visitor.module_checks()

    return function_register
