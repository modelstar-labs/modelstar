import yaml
from yaml.loader import SafeLoader
from modelstar.connectors.snowflake.context_types import SnowflakeConfig

def set_session(config_name: str):
    with open('modelstar.config.yaml') as doc:
        modelstar_config_doc = yaml.load(doc, Loader=SafeLoader)
        
    assert config_name in [ ss.name for ss in modelstar_config_doc['sessions']], f"Missing configuration credentials for: [{target}]"


    # TODO Load from file the sessions


def load_config(target: str) -> SnowflakeConfig:

    with open('modelstar.config.yaml') as doc:
        modelstar_config_doc = yaml.load(doc, Loader=SafeLoader)
        
    assert target in [ ss.name for ss in modelstar_config_doc['sessions']], f"Missing configuration credentials for: [{target}]"

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
