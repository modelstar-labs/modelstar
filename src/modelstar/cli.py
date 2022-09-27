import click
from modelstar.tasks.project import initialize_project, check_project_structure
from modelstar.tasks.database import list_databases
from modelstar.tasks.register import register_function_from_file, register_procedure_from_file
from modelstar.tasks.upload import check_file_path, upload_file
from modelstar.executors.config import load_config
import os


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
@click.pass_context
def init(ctx, projectname: str):
    '''
    modelstar init <project_name>/<. = current folder name>
    creates a folder with the project name using a project template    
    '''

    destination = initialize_project(project_name=projectname)
    click.echo(
        f"\n\tYour project has been created.\n\n\tProject location: {destination}\n")


@main.command("use")
@click.argument("target_config", required=True)
@click.pass_context
def session(ctx, target_config):
    '''
    modelstar use target_confg
        checks if the config.modelstar parameters are right
        connects to the server of snowflake and gets all the database info 
    '''
    check_project_structure()

    click.echo(f"\n\tLoading configuration: [{target_config}]\n")

    config = load_config(target_config)

    response = list_databases(config)

    click.echo(
        f"\n\tShowing available databases for config: [{target_config}]\n")
    click.echo(response)


@main.command("upload")
@click.argument("file_path", required=True)
@click.pass_context
def build(ctx, file_path):
    '''
    modelstar upload <file_path>
        Uploads the file from <file_path> into the cloud location.
    '''
    click.echo(
        f"\n\tChecking file: `{file_path}`")

    abs_file_path = check_file_path(file_path)

    click.echo(
        f"\n\tUploading file: `{abs_file_path}`\n")

    # TODO: get from session
    # TODO Add stage name here, or use default user stage
    # All config options should go into the load_config
    # also pass in the context and set the stage within the load_config
    config = load_config('snowflake')

    response = upload_file(config, file_path)

    click.echo(response)

    click.echo(
        f"\n\tFile available at: `{config.database}.{config.schema}.@{config.stage}`\n")


@main.command("register")
# TODO: change to call_type: function, procedure
@click.argument("function_name", required=True)
# TODO: change to function_file_pointer
# file_path:function_name
@click.argument("file_name", required=True)
@click.pass_context
def build(ctx, function_name, file_name):
    '''
    modelstar register <function_name>
        registers the function that is in the functions folder.
    '''
    click.echo(
        f"\n\tRegistering function: `{function_name}` from `{file_name}`\n")

    check_project_structure()

    # TODO: get from session
    config = load_config('snowflake')

    response = register_function_from_file(config, function_name, file_name)

    click.echo(response)

    click.echo(
        f"\n\tFunction available at: `{config.database}.{config.schema}`\n")


@main.command("procedure")
# TODO: change to function_file_pointer
# file_path:function_name
@click.argument("call_file_pointer", required=True)
@click.pass_context
def build(ctx, call_file_pointer):
    '''
    modelstar register <function_name>
        registers the function that is in the functions folder.
    '''

    pointers = call_file_pointer.split(':')

    file_pointer = pointers[0] + '.py'
    print(os.path.exists(file_pointer))
    print(os.path.isfile(file_pointer))
    call_pointer = pointers[1]
    file_folder_path = os.path.dirname(os.path.abspath(file_pointer))
    print(file_folder_path)

    click.echo(
        f"\n\tRegistering procedure: `{call_pointer}` in `{file_pointer}` \n")

    check_project_structure()

    # TODO: get from session
    config = load_config('snowflake')

    response = register_procedure_from_file(
        config, function_name=call_pointer, file_path=os.path.abspath(file_pointer), file_folder_path=file_folder_path)

    click.echo(response)

    click.echo(
        f"\n\tStored Procedure available at: `{config.database}.{config.schema}`\n")
