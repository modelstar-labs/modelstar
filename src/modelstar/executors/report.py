import os
from jinja2 import Environment, FileSystemLoader

from modelstar.templates import TEMPLATES_PATH

report_templates_folder = os.path.join(TEMPLATES_PATH, 'report')

report_environment = Environment(loader=FileSystemLoader(report_templates_folder))
report_template = report_environment.get_template("report.html.j2")

def write_report_artifacts(artifacts: list, run_id: str):        
    filename = f"report_{run_id}.html"
    file_path = os.path.join(os.getcwd(), filename)
    
    report_context =  { "artifacts": artifacts}
    report_content = report_template.render(report_context)
    
    with open(file_path, mode="w", encoding="utf-8") as report_file:
        report_file.write(report_content)

    return file_path