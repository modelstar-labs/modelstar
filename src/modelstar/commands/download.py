import os


from modelstar.connectors.snowflake.context import SnowflakeContext
from modelstar.connectors.snowflake.context_types import SnowflakeConfig
from modelstar import logger
from modelstar.executors.report import write_artifacts_report


def download_artifacts(config):

    # local_download_path = os.path.join(os.getcwd(), '.modelstar/artifacts/')

    # if not os.path.exists(local_download_path):
    #     os.mkdir(local_download_path)

    # if isinstance(config, SnowflakeConfig):
    #     snowflake_context = SnowflakeContext(config)
    #     response = snowflake_context.get_files(
    #         local_path=local_download_path, name_pattern='.*\.modelstar.joblib.*')
    # else:
    #     raise ValueError(f'Failed to get artifacts.')

    # logger.echo('ML run artifacts files', detail='Downloaded')

    # files_downloaded = response.info['files_downloaded']

    artifacts_downloaded = ['mfOFzRhNW76ItFiT.modelstar.joblib.gz']

    for file in artifacts_downloaded:
        report_file_path = write_artifacts_report(artifacts_file_pointer=file)

        
