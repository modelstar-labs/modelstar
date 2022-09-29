import ast
from dataclasses import dataclass
from typing import List
from modelstar.executors.py_parser.module_import import ModuleImport, parse_import, parse_importfrom
from modelstar.executors.py_parser.module_call import ModelstarCall, parse_modelstar_call
from modelstar.executors.py_parser.module_function import ModuleFunction, parse_function


class ModuleNodeVisitor(ast.NodeVisitor):
    def __init__(self, file_name: str, module_name: str, function_name: str):
        self.file_name: str = file_name
        self.module_name: str = module_name
        self.function_name: str = function_name
        self.imports: List[ModuleImport] = []
        self.functions: List[ModuleFunction] = []
        self.calls: List[ModelstarCall] = []

    def visit_Import(self, node):
        for parsed_info in parse_import(node):
            self.imports.append(parsed_info)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        parsed_info = parse_importfrom(node)
        self.imports.append(parsed_info)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        parsed_info = parse_function(node, self.file_name, self.module_name)
        if parsed_info.name == self.function_name:
            self.functions.append(parsed_info)
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            if node.func.id in ['modelstar_read_path', 'modelstar_write_path']:
                parsed_info = parse_modelstar_call(node)
                self.calls.append(parsed_info)
        self.generic_visit(node)

    def module_checks(self):

        if len(self.functions) != 1:
            raise ValueError(
                f"Function `{self.function_name}` not present in {self.file_name}.")
        else:
            register_function = self.functions[0]
            register_function.check_typing()

        read_files_in_module = []
        for call in self.calls:            
            if call.name == 'modelstar_read_path':
                call.check_paths()
                read_files_in_module.append(call)

        imports_in_module = self.imports

        return FunctionRegister(function=register_function, read_files=read_files_in_module, imports=imports_in_module)


@dataclass
class FunctionRegister():
    function: ModuleFunction
    imports: List[ModuleImport] = None
    read_files: List[ModelstarCall] = None
