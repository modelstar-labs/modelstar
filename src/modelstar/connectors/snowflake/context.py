import snowflake.connector
from dataclasses import dataclass, field
from modelstar.types.table import TableView


@dataclass
class SnowflakeConfig:
    user: str
    account: str
    password: str = field(repr=False)
    database: str = field(default=None, repr=False)
    warehouse: str = field(default=None, repr=False)
    schema: str = field(default=None, repr=False)
    role: str = field(default=None, repr=False)
    port: str = field(default=None, repr=False)
    protocol: str = field(default=None, repr=False)

    def to_dict(self) -> dict:
        if self.protocol is None or self.port is None:
            return {'user': self.user, 'password': self.password, 'account': self.account, 'warehouse': self.warehouse, 'database': self.database, 'schema': self.schema}
        else:
            return {'user': self.user, 'password': self.password, 'account': self.account, 'warehouse': self.warehouse, 'database': self.database, 'schema': self.schema, 'port': self.port, 'protocol': self.protocol}


class SnowflakeContext:
    def __init__(self, config: SnowflakeConfig):
        self.config = config

    def test_connection(self):
        """
        Test the connection and return something useful. 
        """

        sql_statement = ['SELECT PI()', 'SELECT 1 + 1']

        return self.fetch_with_context(sql_statement, fetch=10)

    def set_up(self, conn):
        """
        PURPOSE:
            Create the warehouse, database, schema...
        """

        conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
        conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb_mg")
        conn.cursor().execute("USE DATABASE testdb_mg")
        conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema_mg")

        conn.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")
        conn.cursor().execute("USE DATABASE testdb_mg")
        conn.cursor().execute("USE SCHEMA testdb_mg.testschema_mg")

    def execute_with_context(self, statements, fetch: int = 10):
        # TODO run all the commands within a context manager
        # https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-context-manager-to-connect-and-control-transactions
        # https://stackoverflow.com/questions/72647841/how-to-wrap-each-method-in-a-class-with-a-context-manager
        # https://stackoverflow.com/questions/9213600/function-acting-as-both-decorator-and-context-manager-in-python
        # Connecting to Snowflake using try and except blocks
        con = snowflake.connector.connect(
            autocommit=False, **self.config.to_dict())
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
