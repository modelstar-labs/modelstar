import os
import joblib
from jinja2 import Environment, FileSystemLoader
from modelstar.templates import TEMPLATES_PATH

report_templates_folder = os.path.join(TEMPLATES_PATH, 'report')

report_environment = Environment(
    loader=FileSystemLoader(report_templates_folder))
report_template = report_environment.get_template("report.html.j2")


def write_artifacts_report(artifacts_file_pointer: str):

    artifacts_file_path = os.path.join(
        os.getcwd(), f'.modelstar/artifacts/{artifacts_file_pointer}')

    artifacts = joblib.load(filename=artifacts_file_path)

    report_file_name = f"report_{artifacts.run_id}.html"

    reports_folder = os.path.join(os.getcwd(), '.modelstar/reports')

    if not os.path.exists(reports_folder):
        os.mkdir(reports_folder)

    report_file_path = os.path.join(reports_folder, report_file_name)

    report_content_context = {"artifacts": artifacts}
    report_content = report_template.render(report_content_context)

    with open(report_file_path, mode="w", encoding="utf-8") as report_file:
        report_file.write(report_content)

    return {'report_file_path': report_file_path, 'artifacts_file_path': artifacts_file_path, 'call_name': 'call_name', 'run_id': 'run_id'}
