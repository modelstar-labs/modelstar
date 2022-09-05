import click
import os
from shutil import copytree
from modelstar.templates import TEMPLATES_PATH
import tomlkit
import modelstar.connectors.snowflake as snowflake_connector
import modelstar.executors.parse_module as file_parser
from modelstar.commands.project import initialize_project


@click.group()
@click.option('--database', default=None, help='Target database. Optional: if None, modelstar uses the one set in the project config or prompts one to be created.')
@click.option('--schema', default=None, help='Target schema. Optional: if None, modelstar uses the one set in the project config or prompts one to be created.')
@click.option('--stage', default=None, help='Target stage. Optional: if None, modelstar uses the one set in the project config or prompts one to be created.')
@click.option('--warehouse', default=None, help='Target warehouse. Optional: if None, modelstar uses the one set in the project config or prompts one to be created.')
@click.pass_context
def main(ctx, database, schema, stage, warehouse):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['database'] = database
    ctx.obj['schema'] = schema
    ctx.obj['stage'] = stage
    ctx.obj['warehouse'] = warehouse


@main.command("init")
@click.argument("projectname", required=True, type=str)
def init(projectname: str):
    '''
    modelstar init <project_name>/<. = current folder name>
    creates a folder with the project name using a project template    
    '''

    destination = initialize_project(project_name=projectname)
    click.echo(f"\nYour project has been created.\n\nProject location: {destination}")


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
    sqlc = snowflake_connector.register_from_file(
        snowflake_config, abs_file_path, module_functions[0])
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
