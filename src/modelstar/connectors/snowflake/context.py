import snowflake.connector
from dataclasses import dataclass, field
from modelstar.executors.table import TableView
from modelstar.executors.parse_module import ModuleFunction
from .sql_dialect import register_udf_from_file


@dataclass
class SnowflakeConfig:
    user: str
    account: str
    password: str = field(repr=False)
    database: str = field(default=None, repr=False)
    warehouse: str = field(default=None, repr=False)
    schema: str = field(default=None, repr=False)
    role: str = field(default=None, repr=False)
    stage: str = field(default=None, repr=False)
    port: str = field(default=None, repr=False)
    protocol: str = field(default=None, repr=False)

    def to_connector(self) -> dict:
        if self.protocol is None or self.port is None:
            return {'user': self.user, 'password': self.password, 'account': self.account, 'warehouse': self.warehouse}
        else:
            return {'user': self.user, 'password': self.password, 'account': self.account, 'warehouse': self.warehouse, 'database': self.database, 'schema': self.schema, 'port': self.port, 'protocol': self.protocol}


class SnowflakeContext:
    def __init__(self, config: SnowflakeConfig):
        self.config = config

    def register_udf(self, file_path: str, function: ModuleFunction):
        sql_statements = register_udf_from_file(
            self.config, file_path, function)
        response = self.execute_with_context(sql_statements, fetch=5)

        return response

    def execute_with_context(self, statements, fetch: int = 5):
        # TODO run all the commands within a context manager
        # https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-context-manager-to-connect-and-control-transactions
        # https://stackoverflow.com/questions/72647841/how-to-wrap-each-method-in-a-class-with-a-context-manager
        # https://stackoverflow.com/questions/9213600/function-acting-as-both-decorator-and-context-manager-in-python
        # Connecting to Snowflake using try and except blocks
        con = snowflake.connector.connect(
            autocommit=False, **self.config.to_connector())
        try:
            cur = con.cursor()
            if isinstance(statements, list):
                for stmt in statements:
                    cur.execute(stmt)
            else:
                cur.execute(stmt)
            if fetch is not None:
                table = cur.fetchmany(fetch)
                table_metadata = [col for col in cur.description]
                header = [col.name for col in table_metadata]
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            cur.close()
            con.close()

        if fetch is not None:
            return TableView(table=table,  header=header, metadata=table_metadata)
        else:
            return None
