import os
from typing import List
from dataclasses import dataclass
import pandas as pd
from modelstar.connectors.snowflake.utils import py_to_snow_type


@dataclass
class TableColumn:
    name: str
    py_type: str
    snow_type: str = None

    def __init__(self, name: str, py_type: str):
        self.name = name
        self.py_type = py_type
        self.snow_type = py_to_snow_type(py_type)


@dataclass
class TableInfo():
    name: str
    columns: List[TableColumn]


def table_info_from_csv(file_path: str, table_name: str = None) -> TableInfo:
    if table_name == None:
        file_name = os.path.basename(file_path)
        table_name, _ = os.path.splitext(file_name)

    col_types = col_types_from_csv(file_path)

    return TableInfo(name=table_name, columns=col_types)


def col_types_from_csv(file_path: str):
    df = pd.read_csv(file_path)
    df_dtypes = df.dtypes.to_dict()

    col_types = []
    # Convert column types to SQL types
    for key, value in df_dtypes.items():
        col_name = key

        if value == 'object':
            col_py_type = 'str'
        elif value == 'float64':
            col_py_type = 'float'
        elif value == 'int64':
            col_py_type = 'int'
        else:
            raise ValueError(
                f'`{os.path.basename(file_path)}` contains data type that is not currently supported or in invalid.')

        col_types.append(TableColumn(name=col_name, py_type=col_py_type))

    return col_types
