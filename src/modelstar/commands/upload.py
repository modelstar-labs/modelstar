from modelstar.connectors.snowflake.context import SnowflakeContext
from modelstar.connectors.snowflake.context_types import SnowflakeConfig


def upload_file(config, file_path: str):

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)
        response = snowflake_context.put_file(file_path=file_path)
    else:
        raise ValueError(f'Failed to upload file: {file_path}')

    return response


def upload_folder(config, local_path: str, stage_path: str):

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)
        response = snowflake_context.put_folder(local_path, stage_path)
    else:
        raise ValueError(f'Failed to upload folder: {local_path}')

    return response
