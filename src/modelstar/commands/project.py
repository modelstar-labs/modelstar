import os
from modelstar import logger
from shutil import copytree
from modelstar.templates import TEMPLATES_PATH
import click


def initialize_project(project_name: str) -> str:
    current_working_directory = os.getcwd()
    base_template_path = os.path.join(TEMPLATES_PATH, 'snowflake_project')

    if project_name != '.':
        project_folder_path = os.path.join(
            current_working_directory, project_name)
        try:
            destination = copytree(base_template_path, project_folder_path)
        except FileExistsError as e:
            logger.echo("Project not initialized.")
            logger.echo(f"FileExitsError: {e}")
    else:
        # If the project_name is = ., create the files inside this folder.
        # Check if the files and folders inside this folder don't have the names we are using.
        try:
            destination = copytree(
                base_template_path, current_working_directory)
        except FileExistsError as e:
            logger.echo("Project not initialized.")
            logger.echo(
                f"There are files in the current folder that conflict with the project initialization.")
            logger.echo(f'FileExitsError: {e}')

    return destination
