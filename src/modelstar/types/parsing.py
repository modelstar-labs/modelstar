from dataclasses import dataclass


@dataclass
class ModuleImport:
    '''Class for storing Imports.'''
    module: list
    name: list
    alias: str = None


@dataclass
class FunctionParameter:
    '''Class for storing Function Parameters.'''
    name: str
    type: str


@dataclass
class FunctionReturn:
    '''Class for storing Function Returns.'''
    type: str


@dataclass
class ModuleFunction:
    name: str
    docstring: str
    parameters: 'list[FunctionParameter]'
    returns: 'FunctionReturn'
    # file_name: str
    # line_no: int
    # file_path: str

    def sql_param_list(self) -> str:
        sql_sep = ', '
        param_list = [param.name + ' ' +
                      param.type for param in self.parameters]
        return sql_sep.join(param_list)
