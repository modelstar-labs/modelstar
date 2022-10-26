from modelstar.connectors.snowflake.context import SnowflakeContext
from modelstar.connectors.snowflake.context_types import SnowflakeConfig


def list_databases(config):

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)
        response = snowflake_context.show_databases()

    return response
