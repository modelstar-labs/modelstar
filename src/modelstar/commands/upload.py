from modelstar.connectors.snowflake.context import SnowflakeContext
from modelstar.connectors.snowflake.context_types import SnowflakeConfig

def upload_file(config, file_path: str):

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)
        response = snowflake_context.put_file(file_path=file_path)
    else:
        raise ValueError(f'Failed to upload file: {file_path}')

    return response
