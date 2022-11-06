import yaml
import os
from yaml.loader import SafeLoader
from modelstar.connectors.snowflake.context_types import SnowflakeConfig
from jsonschema import validate
from modelstar.version import __version__
from modelstar.utils.path import if_exists_else_create_file_folder

config_schema = {
    "type": "object",
    "required": [
        "account",
        "username",
        "password",
        "database",
        "schema",
        "stage",
        "warehouse"
    ],
    "properties": {
        "account": {
            "type": "string"
        },
        "username": {
            "type": "string"
        },
        "password": {
            "type": "string"
        },
        "database": {
            "type": "string"
        },
        "schema": {
            "type": "string"
        },
        "stage": {
            "type": "string"
        },
        "warehosue": {
            "type": "string"
        }
    }
}


def set_session(config_name: str) -> None:
    with open('modelstar.config.yaml') as doc:
        modelstar_config_doc = yaml.load(doc, Loader=SafeLoader)

    assert config_name in [ss['name'] for ss in modelstar_config_doc['sessions']
                           ], f"Missing session configuration for: `{config_name}`"

    for config in modelstar_config_doc['sessions']:
        session_name = config.get('name')
        if session_name == config_name:
            assert config.get(
                'connector') == 'snowflake', 'Connectors supprted at this moment is only `snowflake`.'

            session_config = config.get('config')

            validate(instance=session_config, schema=config_schema)

            session_config_yaml = os.path.join(
                os.getcwd(), '.modelstar/session.config.yaml')

            if_exists_else_create_file_folder(
                ff_path=session_config_yaml, ff_type='file')

            with open(session_config_yaml, 'w') as session_file:
                session_file.write('# MODELSTAR INTERNAL FILE: SESSION\n')
                session_file.write('---\n')
                dump_content = {'modelstar': {
                    'version': __version__}, 'session': config}
                yaml.dump(dump_content, session_file,
                          sort_keys=False, default_flow_style=False)


def load_config() -> SnowflakeConfig:

    session_config_yaml = os.path.join(
        os.getcwd(), '.modelstar/session.config.yaml')

    if not os.path.exists(session_config_yaml):
        raise ValueError(
            'No session has been initialized. You must initialize a session using  `modelstar use <session_name>`.')

    with open('.modelstar/session.config.yaml') as session_file:
        session_doc = yaml.load(session_file, Loader=SafeLoader)

    assert 'session' in session_doc, 'No session has been initialized. You must initialize a session using  `modelstar use <session_name>`.'

    session_info = session_doc.get('session')
    session_config = session_info.get('config')

    assert session_info.get(
        'connector') == 'snowflake', 'Connectors supprted at this moment is only `snowflake`.'
    validate(instance=session_config, schema=config_schema)

    account = session_config.get("account")
    user = session_config.get("username")
    password = session_config.get("password")
    database = session_config.get("database")
    schema = session_config.get("schema")
    warehouse = session_config.get("warehouse")
    stage = session_config.get("stage")
    role = session_config.get("role", None)

    config = SnowflakeConfig(name=session_info['name'], user=user, account=account, password=password,
                             database=database, warehouse=warehouse, schema=schema, role=role, stage=stage)

    return config
