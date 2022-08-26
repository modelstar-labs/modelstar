import click
import os
from shutil import copytree
from modelstar.templates import TEMPLATES_PATH
import tomlkit


@click.group()
def main():
    pass


@main.command("init")
@click.argument("projectname", required=True, type=str)
def init(projectname):
    '''
    modelstar init <project_name>/<. = current folder name>
    creates a folder with the project name
    creates files inside
        config.modelstar
        .modelstar/databases/....info after connecting
        /models/model_1.sql
        /functions/example.py
    '''

    current_working_directory = os.getcwd()
    # current_folder_name = os.path.basename(current_working_directory)
    base_template_path = os.path.join(TEMPLATES_PATH, 'snowflake_project')

    if projectname != '.':
        project_folder_path = os.path.join(
            current_working_directory, projectname)
        try:
            destination = copytree(base_template_path, project_folder_path)
            click.echo(
                f"Yout project has been created.\nProject location: {destination}")
        except FileExistsError as e:
            click.echo(f"Project not initialized.\n\nFileExitsError: {e}")
    else:
        # If the projectname is = ., create the files inside this folder.
        # Check if the files and folders inside this folder don't have the names we are using.
        try:
            destination = copytree(
                base_template_path, current_working_directory)
            click.echo(
                f"Yout project has been created.\nProject location: {destination}")
        except FileExistsError as e:
            click.echo(
                f"Project not initialized. There are files in the current folder that conflict with the project initialization.\n\nFileExitsError: {e}")


@main.command("test")
@click.argument("target", required=True)
def test(target):
    '''
    modelstar connect
        checks if the config.modelstar parameters are right
        connects to the server of snowflake and gets all the database info 
    '''
    click.echo(f"Test: {target}")

    cwd = os.getcwd()
    project_list_set = set(['modelstar_project.toml', 'models', '.modelstar'])
    cwd_list_set = set(os.listdir(cwd))

    assert cwd_list_set.issubset(
        project_list_set), "Missing files, or not a modelstar project"

    assert 'modelstar_project.toml' in cwd_list_set, "Missing modelstrat_project.toml configuration file."

    modelstar_config_doc = tomlkit.parse(
        open('./modelstar_project.toml').read())

    assert "snowflake" in modelstar_config_doc, "Missing Snowflake credentials."

    account = modelstar_config_doc.get("account")
    username = modelstar_config_doc.get("username")
    password = modelstar_config_doc.get("password")
    database = modelstar_config_doc.get("database")
    warehouse = modelstar_config_doc.get("warehouse")
    role = modelstar_config_doc.get("role")


@main.command("build")
@click.argument("target", required=True)
def build(target):
    '''
    modelstar build <model_name>
        builds the model in the <model_name>.py script
        deploys the model for inference
    '''
    print(f"Build: {target}")


@main.command("run")
def run():
    '''
    modelstar run/start/launch
        starts the interactive server interface
    '''
    print(f"Run")
