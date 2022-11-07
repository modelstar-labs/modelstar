import os
import yaml
import click
from typing import Union
from yaml.loader import SafeLoader
from jsonschema import validate
from modelstar.connectors.snowflake.context import SnowflakeResponse
from modelstar.version import __version__
from dataclasses import dataclass


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


@dataclass
class RunRecord:
    run_id: str


class SessionRegistry:
    def __init__(self, log_status: bool = True):
        self.log_status = log_status
        self.file_path = None
        self.registrations = []
        self.runs = []

    def load_registry(self):
        self.file_path = os.path.join(
            os.getcwd(), '.modelstar/session.registry.yaml')

        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as doc:
                doc.write('# MODELSTAR INTERNAL FILE: SESSION REGISTRY\n')
                doc.write('---\n')
                dump_init = {'modelstar': {
                    'version': __version__}, 'registrations': [], 'runs': []}
                yaml.dump(dump_init, doc, sort_keys=False,
                          default_flow_style=False)

        with open(self.file_path) as doc:
            load_doc = yaml.load(doc, Loader=SafeLoader)
            self.registrations = load_doc['registrations']
            self.runs = load_doc['runs']

    def dump_registry(self):
        self.file_path = os.path.join(
            os.getcwd(), '.modelstar/session.registry.yaml')

        with open(self.file_path, 'w') as doc:
            doc.write('# MODELSTAR INTERNAL FILE: SESSION REGISTRY\n')
            doc.write('---\n')
            dump_doc = {'modelstar': {'version': __version__},
                        'registrations': self.registrations, 'runs': self.runs}
            yaml.dump(dump_doc, doc, sort_keys=False, default_flow_style=False)

    def add_record(self, run_record: dict):
        call_name = run_record['call_name']
        call_version = run_record['call_version']
        run_id = run_record['run_id']
        registrations = self.registrations

        find_regis = (i for i, e in enumerate(
            registrations) if (e['name'] == call_name and e['version'] == call_version))

        idx_regis = next(find_regis)
        registrations[idx_regis]['runs'].append(run_id)
        registrations[idx_regis]['records'].append({'run_id': run_record['run_id'], 'report_file_path': run_record['report_file_path'],
                                                   'run_record_file_path': run_record['run_record_file_path'], 'run_timestamp': run_record['run_timestamp']})
        self.registrations = registrations
        self.runs.append({'run_id': run_record['run_id'], 'report_file_path': run_record['report_file_path'],
                         'run_record_file_path': run_record['run_record_file_path']})

    def add_register(self, name: str, type: str, version: str):

        try:
            find_regis = (i for i, e in enumerate(self.registrations) if (
                e['name'] == name and e['version'] == version))
            next(find_regis)
        except:
            registration = {'name': name, 'type': type,
                            'version': version, 'runs': [], 'records': []}

            self.registrations.append(registration)
