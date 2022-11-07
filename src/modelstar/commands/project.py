import os
import yaml
from modelstar import logger
from shutil import copytree
from modelstar.templates import TEMPLATES_PATH


def initialize_project(project_name: str) -> str:
    current_working_directory = os.getcwd()
    base_template_path = os.path.join(TEMPLATES_PATH, 'snowflake_project')

    if project_name != '.':
        project_folder_path = os.path.join(
            current_working_directory, project_name)
        try:
            destination = copytree(base_template_path, project_folder_path)
            os.mkdir(os.path.join(project_folder_path, '.modelstar'))

        except FileExistsError as e:
            logger.echo("Project not initialized.")
            logger.echo(
                f"There are files in the current folder that conflict with the project initialization.")
            raise FileExistsError(e)
    else:
        # If the project_name is = ., create the files inside this folder.
        # TODO: Check if the files and folders inside this folder don't have the names we are using.
        try:
            destination = current_working_directory

            with open(os.path.join(current_working_directory, 'modelstar.config.yaml'), 'w') as file:
                file.write('# MODELSTAR INTERNAL FILE: SESSION\n')
                file.write('---\n')
                dump_content = {'sessions': [{'name': 'snowflake-demo-config', 'connector': 'snowflake', 'config': {'account': '<account>', 'username': '<username>',
                                                                                                                    'password': '<password>', 'database': '<database>', 'schema': '<schema>', 'stage': '<stage>', 'warehouse': '<warehouse>'}}]}
                yaml.dump(dump_content, file, sort_keys=False,
                          default_flow_style=False)

            os.mkdir(os.path.join(current_working_directory, '.modelstar'))

        except FileExistsError as e:
            logger.echo("Project not initialized.")
            logger.echo(
                f"There are files in the current folder that conflict with the project initialization.")
            raise FileExistsError(e)

    return destination
