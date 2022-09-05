import click
import os
from shutil import copytree
from modelstar.templates import TEMPLATES_PATH
import tomlkit
import modelstar.connectors.snowflake as snowflake_connector
import modelstar.executors.parse_module as file_parser


@click.group()
# @click.option('--database', default=None, help='Target database. Optional: uses the default set in the project config.')
# @click.option('--schema', default=None, help='Target schema. Optional: uses the default set in the project config.')
# @click.option('--stage', default=None, help='Target stage. Optional: uses the default set in the project config.')
# @click.option('--warehouse', default=None, help='Target warehouse. Optional: uses the default set in the project config.')
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

    snowflake_config = modelstar_config_doc.get('snowflake')

    account = snowflake_config.get("account")
    username = snowflake_config.get("username")
    password = snowflake_config.get("password")
    database = snowflake_config.get("database")
    warehouse = snowflake_config.get("warehouse")
    role = snowflake_config.get("role")

    version = snowflake_connector.test_connection(snowflake_config)
    click.echo(version)

    # os.walk to get files and dirs: https://www.tutorialspoint.com/python/os_walk.htm


@main.command("register")
@click.argument("file_name", required=True)
@click.argument("function_name", required=True)
def build(file_name, function_name, **kwargs):
    '''
    modelstar register <function_name>
        registers the function that is in the functions folder.
    '''
    click.echo(f"Registering `{file_name}` function... ")

    target = file_name.strip()
    # Clean target : check if a .py file or a path to a .py file.
    current_working_directory = os.getcwd()
    functions_folder_path = os.path.join(
        current_working_directory, 'functions')
    target_function_path = os.path.join(functions_folder_path, target)

    if not os.path.exists(target_function_path):
        raise ValueError(f"{target} does not exist")

    click.echo(f'{target} check done.')
    abs_file_path = os.path.abspath(target_function_path)
    click.echo(f'{abs_file_path}')

    modelstar_config_doc = tomlkit.parse(
        open('./modelstar_project.toml').read())

    assert "snowflake" in modelstar_config_doc, "Missing Snowflake credentials."

    snowflake_config = modelstar_config_doc.get('snowflake')

    # Get the imports and function list. 
    # send -> abs file path and get all the info about the functions and imports. 
    module_functions = file_parser.get_info(abs_file_path)
    functions_in_modules = [func.name for func in module_functions]
    if function_name not in functions_in_modules:
        raise ValueError
    
    # click.echo(module_functions)
    
    # TODO Add stage name here, or use default user stage 
    # snowflake_connector.register_udf(snowflake_config, 'addition')
    sqlc = snowflake_connector.register_from_file(snowflake_config, abs_file_path, module_functions[0])
    click.echo(sqlc)
    

    # with open(target_function_path, "r") as source:
    #     tree = ast.parse(source.read())
    #     click.echo(tree)


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
