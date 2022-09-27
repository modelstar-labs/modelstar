import tomlkit
from modelstar.connectors.snowflake.context import SnowflakeConfig


def load_config(target: str):
    modelstar_config_doc = tomlkit.parse(
        open('./modelstar.toml').read())

    assert target in modelstar_config_doc, f"Missing configuration credentials for: [{target}]"

    # TODO use trarge there and load the target from the .modelstar/session
    target_config_doc = modelstar_config_doc.get('snowflake')

    account = target_config_doc.get("account")
    user = target_config_doc.get("username")
    password = target_config_doc.get("password")
    database = target_config_doc.get("database")
    schema = target_config_doc.get("schema")
    warehouse = target_config_doc.get("warehouse")
    stage = target_config_doc.get("stage")
    role = target_config_doc.get("role")

    config = SnowflakeConfig(user=user, account=account, password=password,
                             database=database, warehouse=warehouse, schema=schema, role=role, stage=stage)

    return config
