from dataclasses import dataclass
from dataclasses import dataclass, field
from modelstar.utils.response import TableView


@dataclass
class SnowflakeConfig:
    name: str
    user: str
    account: str
    password: str = field(repr=False)
    database: str = field(repr=False)
    schema: str = field(repr=False)
    stage: str = field(repr=False)
    warehouse: str = field(repr=False)
    role: str = field(default=None, repr=False)
    port: str = field(default=None, repr=False)
    protocol: str = field(default=None, repr=False)

    def to_connector(self) -> dict:
        if self.protocol is None or self.port is None:
            return {'user': self.user, 'password': self.password, 'account': self.account, 'warehouse': self.warehouse}
        else:
            return {'user': self.user, 'password': self.password, 'account': self.account, 'warehouse': self.warehouse, 'database': self.database, 'schema': self.schema, 'port': self.port, 'protocol': self.protocol}


@dataclass
class SnowflakeResponse:
    table: TableView = None
    info: dict = None


@dataclass
class FileFormat:
    format_name: str
    format_type: str
    delimiter: str
    skip_header: int


type_map_parameter = {'int': 'NUMBER', 'str': 'STRING', 'float': 'FLOAT', 'bool': 'BOOL',
                      'bytes': 'BINARY', 'list': 'ARRAY', 'dict': 'OBJECT', 'DataFrame': 'STRING'}

type_map_return = {'int': 'NUMBER', 'str': 'STRING', 'float': 'FLOAT', 'bool': 'BOOL',
                   'bytes': 'BINARY', 'list': 'ARRAY', 'dict': 'OBJECT', 'DataFrame': 'VARIANT'}
