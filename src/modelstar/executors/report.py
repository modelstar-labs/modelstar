import os
import joblib
from jinja2 import Environment, FileSystemLoader
from modelstar.templates import TEMPLATES_PATH
from modelstar.utils.path import if_exists_else_create_file_folder

report_templates_folder = os.path.join(TEMPLATES_PATH, 'report')

report_environment = Environment(
    loader=FileSystemLoader(report_templates_folder))
report_template = report_environment.get_template("report.html")


def prepare_run_record_report(run_record_file_pointer: str):

    run_record_file_path = os.path.join(
        os.getcwd(), f'.modelstar/records/{run_record_file_pointer}')

    run_record = joblib.load(filename=run_record_file_path)

    report_file_name = f"report_{run_record['run_id']}.html"

    reports_folder = os.path.join(os.getcwd(), '.modelstar/reports')

    if_exists_else_create_file_folder(ff_path=reports_folder, ff_type='folder')

    report_file_path = os.path.join(reports_folder, report_file_name)

    report_content_context = run_record
    report_content = report_template.render(report_content_context)

    with open(report_file_path, mode="w", encoding="utf-8") as report_file:
        report_file.write(report_content)

    return {'report_file_path': report_file_path, 'run_record_file_path': run_record_file_path, 'call_name': run_record['call_name'], 'call_version': run_record['call_version'], 'run_id': run_record['run_id'], 'run_timestamp': run_record['run_timestamp']}
