import snowflake.connector
from dataclasses import dataclass, field
from modelstar.executors.table import TableView
from modelstar.executors.py_parser.module_function import ModuleFunction
from .sql_dialect import register_udf_from_file, register_procedure_from_file, put_file_from_local, clear_function_stage_files
from typing import List
import os


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


@dataclass
class SnowflakeResponse:
    table: TableView = None
    info: dict = None


class SnowflakeContext:
    def __init__(self, config: SnowflakeConfig):
        self.config = config

    def register_udf(self, file_path: str, function: ModuleFunction, imports: list, package_imports: list, version: str = None) -> SnowflakeResponse:

        sql_statements = register_udf_from_file(
            self.config, file_path, function, imports, package_imports, version)
        table = self.execute_with_context(sql_statements, fetch=5)

        return SnowflakeResponse(table=table)

    def register_procedure(self, file_path: str, function: ModuleFunction, imports: list, package_imports: list, version: str = None) -> SnowflakeResponse:

        sql_statements = register_procedure_from_file(
            self.config, file_path, function, imports, package_imports, version)
        table = self.execute_with_context(sql_statements, fetch=5)

        return SnowflakeResponse(table=table)

    def clear_existing_function_version(self, function: ModuleFunction, version: str) -> None:
        sql_statements = clear_function_stage_files(
            self.config, function_name=function.name, version=version)
        self.execute_with_context(sql_statements, fetch=None)

    def put_file(self, file_path: str, stage_path: str = None) -> SnowflakeResponse:
        sql_statements = put_file_from_local(
            self.config, file_path, stage_path)
        table = self.execute_with_context(sql_statements, fetch=5)

        file_name = os.path.basename(file_path)
        if stage_path is not None:
            file_stage_path = f'@{self.config.stage}/{stage_path}/{file_name}'
        else:
            file_stage_path = f'@{self.config.stage}/{file_name}'

        return SnowflakeResponse(table=table, info={'file_stage_path': file_stage_path})

    def put_multi_file(self, file_paths: List[str], stage_path: str = None) -> SnowflakeResponse:
        pass
        # sql_statements = put_file_from_local(self.config, file_path)
        # table = self.execute_with_context(sql_statements, fetch=5)

        # return SnowflakeResponse(table=table)

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
