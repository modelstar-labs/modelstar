import os
from modelstar.connectors.snowflake.context import SnowflakeContext
from modelstar.connectors.snowflake.context_types import SnowflakeConfig
from modelstar import logger, session_registry
from modelstar.executors.report import prepare_run_record_report
from modelstar.utils.report import view_in_browser
from modelstar.utils.path import if_exists_else_create_file_folder


def view_download_records(config, run_id: str):

    report_path = None

    if len(session_registry.runs) > 0:
        for run in session_registry.runs:
            if run['run_id'] == run_id:
                report_path = run['report_file_path']

    if report_path is None:

        local_download_path = os.path.join(os.getcwd(), '.modelstar/records/')

        if_exists_else_create_file_folder(
            ff_path=local_download_path, ff_type='folder')

        if isinstance(config, SnowflakeConfig):
            snowflake_context = SnowflakeContext(config)
            response = snowflake_context.get_files(
                local_path=local_download_path, name_pattern='.*\.modelstar.joblib.*')
        else:
            raise ValueError(f'Failed to get run records.')

        logger.echo('Snowflake run records', detail='Downloaded')

        records_downloaded = response.info['files_downloaded']

        for file in records_downloaded:
            record_info = prepare_run_record_report(
                run_record_file_pointer=file)
            session_registry.add_record(record_info)

        if len(session_registry.runs) > 0:
            for run in session_registry.runs:
                if run['run_id'] == run_id:
                    report_path = run['report_file_path']

        if report_path is not None:
            view_in_browser(file_path=report_path)
        else:
            raise ValueError(f'Run report for {run_id} not found.')

    else:
        view_in_browser(file_path=report_path)


def build_new_report(run_id):

    record_info = prepare_run_record_report(
        run_record_file_pointer='N6BVgs8YHko9Xp8H.modelstar.joblib.gz')
    view_in_browser(file_path=record_info['report_file_path'])
