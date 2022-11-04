import os
import yaml
import click
from typing import Union
from yaml.loader import SafeLoader
from jsonschema import validate
from modelstar.connectors.snowflake.context import SnowflakeResponse
from modelstar.version import __version__


class Logger:
    '''
    Create a logger context, that can output to the console and also store it in a file.
    The option can be toggled using an environment variable. 

    Information to append to the logger:
        DEBUG
        INFO
        WARNING
        ERROR
        CRITICAL

    Options for it to have:
        - [ ] print
        - [ ] add to file
    '''

    def __init__(self, console_log: bool = True, file_log: bool = False):
        self.console_log = console_log
        self.file_log = file_log

    def echo(self, msg: Union[str, SnowflakeResponse], detail: str = None):
        if detail == None:
            if isinstance(msg, str):
                print(f'\n  {msg}')
            elif isinstance(msg, SnowflakeResponse):
                print('')
                print(msg.table.print())
        else:
            print(f'\n  {msg}:  {detail}')


class SessionRegistry:
    def __init__(self, log_status: bool = True):
        self.log_status = log_status
        self.file_path = None
        self.doc = []
        """
        {
            'modelstar': {
                'version': '0.1.0'
            },
            'registrations': [
                {
                    'name': 'something',
                    'type': 'procedure/function',
                    'version': 'v1'
                    'runs': [...run_ids],
                    'artifacts': [
                        'run_id': 'asdasd',
                        'artifact_path': '/asdasldkj/asdasd.modelstar.joblib',
                        'report_path': '/asdasldkj/asdasd.report',
                    ]
                }
            ]
        }
        
        """
        self.load_registry()

    def load_registry(self):
        self.file_path = os.path.join(
            os.getcwd(), '.modelstar/session.registry.yaml')

        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as doc:
                doc.write('# MODELSTAR INTERNAL FILE: SESSION REGISTRY\n')
                doc.write('---\n')
                dump_init = {'modelstar': {
                    'version': __version__}, 'registrations': []}
                yaml.dump(dump_init, doc, sort_keys=False,
                          default_flow_style=False)

        with open(self.file_path) as doc:
            load_doc = yaml.load(doc, Loader=SafeLoader)
            self.doc = load_doc['registrations']

    def dump_registry(self):
        self.file_path = os.path.join(
            os.getcwd(), '.modelstar/session.registry.yaml')

        with open(self.file_path, 'w') as doc:
            doc.write('# MODELSTAR INTERNAL FILE: SESSION REGISTRY\n')
            doc.write('---\n')
            dump_doc = {'modelstar': {'version': __version__},
                        'registrations': self.doc}
            yaml.dump(dump_doc, doc, sort_keys=False, default_flow_style=False)

    def add_artifact(self, artifact: dict):
        self.doc.append(artifact)

    def add_register(self, name: str, type: str, version: str):
        '''
        {
                    'name': 'something',
                    'type': 'procedure/function',
                    'version': 'v1'
                    'runs': [...run_ids],
                    'artifacts': [
                        'run_id': 'asdasd',
                        'artifact_path': '/asdasldkj/asdasd.modelstar.joblib',
                        'report_path': '/asdasldkj/asdasd.report',
                    ]
                }
        '''

        registration = {'name': name, 'type': type,
                        'version': version, 'runs': [], 'artifacts': []}

        self.doc.append(registration)
