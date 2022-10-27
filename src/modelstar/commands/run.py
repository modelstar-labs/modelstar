from modelstar.connectors.snowflake.context_types import SnowflakeConfig, SnowflakeResponse
from modelstar.connectors.snowflake.context import SnowflakeContext


def run_sql(config: SnowflakeConfig, sql: str) -> SnowflakeResponse:

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)
        response = snowflake_context.run_sql(statements=sql)
    else:
        raise ValueError(f'Failed to execute the SQL statements')

    return response
