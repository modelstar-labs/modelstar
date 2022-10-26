import ast
import os
from dataclasses import dataclass


@dataclass
class ModelstarCall:
    name: str
    local_path: str = None
    snowflake_path: str = None

    def check_paths(self):
        if self.local_path == None:
            raise ValueError(
                'FilePathError: `local_path` parameter value missing.')
        # if self.snowflake_path == None:
        #     raise ValueError(
        #         'FilePathError: `snowflake_path` parameter value missing.')

        if os.path.exists(self.local_path):
            self.local_path = os.path.abspath(self.local_path)
        else:
            raise ValueError(
                'FilePathError: File with `local_path` does not exist. Tip: Pass the absolute path.')


def parse_modelstar_call(node):
    call_name = node.func.id

    # Make sure the args are none:
    # TODO: Raise Error when args are entered instead of keywords.

    local_path = None
    snowflake_path = None

    for kw in node.keywords:

        if kw.arg == 'local_path':
            if isinstance(kw.value, ast.Constant):
                path = kw.value.value
            else:
                raise ValueError(
                    'Only constants allowed. Enter the path as a string.')
            local_path = path

        if kw.arg == 'snowflake_path':
            if isinstance(kw.value, ast.Constant):
                path = kw.value.value
            else:
                raise ValueError(
                    'Only constants allowed. Enter the path as a string.')

            snowflake_path = path

    return ModelstarCall(name=call_name, local_path=local_path, snowflake_path=snowflake_path)
