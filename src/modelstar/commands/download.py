import os
import webbrowser
from modelstar.connectors.snowflake.context import SnowflakeContext
from modelstar.connectors.snowflake.context_types import SnowflakeConfig
from snowflake.connector.errors import OperationalError
import joblib
from modelstar.executors.report import write_report_artifacts


def download_artifacts(config):

    local_folder_path = os.path.join(os.getcwd(), '.modelstar/artifacts/')

    if not os.path.exists(local_folder_path):
        os.mkdir(local_folder_path)

    '''
    GET internalStage file://<path_to_file>/<filename>
        [ PARALLEL = <integer> ]
        [ PATTERN = '<regex_pattern>'' ]
    
    To replicate:

    Download all files in the stage for the mytable table to the /tmp/data local directory: 
        get @%mytable file:///tmp/data/;    
    
    Download files from the myfiles path in the stage for the current user to the /tmp/data local directory: 
        get @~/myfiles file:///tmp/data/;
    
    '''

    # sql = f"GET @{config.stage} file://{local_folder_path} PATTERN='.*\.modelstar.joblib.*'"

    # if isinstance(config, SnowflakeConfig):
    #     try:
    #         snowflake_context = SnowflakeContext(config)
    #         response = snowflake_context.run_sql(statements=sql)
    #         print(response.table.table)

    #         return response
    #     except OperationalError:
    #         print('Oops')
    #     except:
    #         raise ValueError(f'Failed to execute check.')
    # else:
    #     raise ValueError(f'Failed to execute.')

    file = 'mfOFzRhNW76ItFiT.modelstar.joblib.gz'
    file_path = os.path.join(os.getcwd(), f'.modelstar/artifacts/{file}')
    artifacts = joblib.load(filename=file_path)

    for artifact in artifacts:
        # dict_keys(['name', 'html'])
        print(artifact.keys())

    local_path = write_report_artifacts(artifacts=artifacts, run_id='mfOFzRhNW76ItFiT')

    local_url = 'file://' + local_path
    webbrowser.open(local_url, new=2)
