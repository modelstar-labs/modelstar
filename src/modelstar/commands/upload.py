import os
from modelstar.connectors.snowflake.context import SnowflakeContext, SnowflakeConfig


def check_file_path(file_path: str) -> str:
    file_path = file_path.strip()
    abs_file_path = os.path.abspath(file_path)

    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(
            f"Unable to locate {file_path} or it does not exist. Tip: provide an absolute path.")

    return abs_file_path


def upload_file(config, file_path: str):

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)
        response = snowflake_context.put_file(file_path=file_path)
    else:
        raise ValueError(f'Failed to upload file: {file_path}')

    return response.table.print()
