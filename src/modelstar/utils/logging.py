from modelstar.connectors.snowflake.context import SnowflakeResponse
from typing import Union
import click

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


class Logger:
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
