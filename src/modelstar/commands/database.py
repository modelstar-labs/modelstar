from modelstar.connectors.snowflake.context import SnowflakeContext, SnowflakeConfig


def list_databases(config):

    if isinstance(config, SnowflakeConfig):
        snowflake_context = SnowflakeContext(config)
        table = snowflake_context.execute_with_context(['SHOW DATABASES'])
        response = table.print(cols=['created_on', 'name', 'origin', 'owner'])

    return response
