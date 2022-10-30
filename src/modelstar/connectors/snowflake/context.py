from snowflake.connector.pandas_tools import write_pandas
import pandas
import os
from pandas import DataFrame
from typing import List, Union
import snowflake.connector
from modelstar.utils.response import TableView
from modelstar.executors.py_parser.module_function import ModuleFunction
import modelstar.connectors.snowflake.sql_dialect as SQL
from modelstar.executors.table import TableInfo
from modelstar.connectors.snowflake.context_types import SnowflakeConfig, SnowflakeResponse, FileFormat


class SnowflakeContext:
    def __init__(self, config: SnowflakeConfig):
        self.config = config

    def run_sql(self, statements: Union[str, list]):
        sql_statements_0 = SQL.session_use(self.config)

        if isinstance(statements, str):
            sql_statements_1 = [statements]
        elif isinstance(statements, list):
            sql_statements_1 = statements
        else:
            raise ValueError(f'Cannot execute {statements}')

        sql_statements = sql_statements_0 + sql_statements_1

        response_table = self.execute_with_context(sql_statements, fetch=20)

        return SnowflakeResponse(table=response_table)

    def show_databases(self) -> SnowflakeResponse:
        sql_statements = ['SHOW DATABASES']
        response_table = self.execute_with_context(sql_statements, fetch='all')
        response_table.display_cols = ['created_on', 'name', 'origin', 'owner']

        return SnowflakeResponse(table=response_table)

    def register_udf(self, file_path: str, function: ModuleFunction, imports: list, package_imports: list, version: str = None) -> SnowflakeResponse:
        sql_statements_0 = SQL.session_use(self.config)
        sql_statements_1 = SQL.register_udf_from_file(
            self.config, file_path, function, imports, package_imports, version)
        sql_statements = sql_statements_0 + sql_statements_1
        response_table = self.execute_with_context(sql_statements, fetch=5)

        return SnowflakeResponse(table=response_table)

    def register_procedure(self, file_path: str, function: ModuleFunction, imports: list, package_imports: list, version: str = None) -> SnowflakeResponse:
        sql_statements_0 = SQL.session_use(self.config)
        sql_statements_1 = SQL.register_procedure_from_file(
            self.config, file_path, function, imports, package_imports, version)
        sql_statements = sql_statements_0 + sql_statements_1
        response_table = self.execute_with_context(sql_statements, fetch=5)

        return SnowflakeResponse(table=response_table)

    def clear_existing_function_version(self, function: ModuleFunction, version: str) -> None:
        sql_statements_0 = SQL.session_use(self.config)
        sql_statements_1 = SQL.clear_function_stage_files(
            self.config, function_name=function.name, version=version)
        sql_statements = sql_statements_0 + sql_statements_1
        self.execute_with_context(sql_statements, fetch=None)

    def put_file(self, file_path: str, stage_path: str = None) -> SnowflakeResponse:
        sql_statements_0 = SQL.session_use(self.config)
        sql_statements_1 = SQL.put_file_from_local(
            self.config, file_path, stage_path)

        sql_statements = sql_statements_0 + sql_statements_1
        response_table = self.execute_with_context(sql_statements, fetch=5)

        file_name = os.path.basename(file_path)
        if stage_path is not None:
            file_stage_path = f'@{self.config.stage}/{stage_path}/{file_name}'
        else:
            file_stage_path = f'@{self.config.stage}/{file_name}'

        return SnowflakeResponse(table=response_table, info={'file_stage_path': file_stage_path})

    def create_table_from_csv(self, file_path: str, table_info: TableInfo, file_format: FileFormat) -> SnowflakeResponse:
        '''
        Operations performed:
        1. Uploads the file from local to stage using: PUT
        2. Creates the table with the column types using: CREATE TABLE
        3. Creates the file format for copying the data from file to table using: CREATE FILE_FORMAT
        4. Copys the data from stage file into table using: COPY INTO
        '''
        sql_statements_0 = SQL.session_use(self.config)

        sql_statements_1 = SQL.put_file_from_local(
            self.config, file_path=file_path, stage_path=None)

        file_name = os.path.basename(file_path)
        file_stage_path = f'@{self.config.stage}/{file_name}'

        sql_statements_2 = SQL.create_table(self.config, table_info=table_info)

        sql_statements_3 = SQL.create_file_format(
            self.config, file_format=file_format)

        sql_statements_4 = SQL.copy_file_into_table(
            self.config, table_info=table_info, file_stage_path=file_stage_path, file_format=file_format)

        sql_statements = sql_statements_0 + sql_statements_1 + \
            sql_statements_2 + sql_statements_3 + sql_statements_4

        response_table = self.execute_with_context(sql_statements, fetch=5)
        response_table.display_cols = [
            'file', 'status', 'rows_parsed', 'rows_loaded']

        return SnowflakeResponse(table=response_table)

    def put_multi_file(self, file_paths: List[str], stage_path: str = None) -> SnowflakeResponse:
        sql_statements_0 = SQL.session_use(self.config)
        sql_statements_1 = []

        for file_path in file_paths:
            sql_statements_1 = sql_statements_1 + \
                SQL.put_file_from_local(self.config, file_path, stage_path)

        sql_statements = sql_statements_0 + sql_statements_1
        response_table = self.execute_with_context(sql_statements, fetch=5)

        file_stage_paths = []
        for file_path in file_paths:
            file_name = os.path.basename(file_path)

            if stage_path is not None:
                file_stage_path = f'@{self.config.stage}/{stage_path}/{file_name}'
            else:
                file_stage_path = f'@{self.config.stage}/{file_name}'

            file_stage_paths.append(file_stage_path)

        return SnowflakeResponse(table=response_table, info={'file_stage_paths': file_stage_paths})

    def execute_with_context(self, statements, fetch: Union[int, str] = 5):
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
                if isinstance(fetch, int):
                    table = cur.fetchmany(fetch)
                    table_metadata = [col for col in cur.description]
                    header = [col.name for col in table_metadata]
                elif fetch == 'all':
                    table = cur.fetchall()
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

    def write_dataframe_as_table(self, df: DataFrame, table_name: str):

        # Connecting to Snowflake using try and except blocks
        cnx = snowflake.connector.connect(**self.config.to_connector())

        # Write the data from the DataFrame to the table named "customers".
        success, nchunks, nrows, output = write_pandas(
            cnx, df, table_name, database=self.config.database, schema=self.config.schema, overwrite= True, auto_create_table=True)

        print(output)
        print(type(output))

        return success
