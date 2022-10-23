import click
from modelstar.commands.project import initialize_project, check_project_structure
from modelstar.commands.database import list_databases
from modelstar.commands.register import register_function_from_file, register_procedure_from_file
from modelstar.commands.upload import upload_file
from modelstar.commands.create import create_table
from modelstar.executors.config import load_config
from modelstar.utils.path import strip_file_namespace_pointer, check_file_path


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
    # check if a yaml file is present and if a .modelstar is present. If not make only a .modelstar folder.
    check_project_structure()

    click.echo(f"\n\tLoading configuration: [{target_config}]\n")

    # TODO: add this session details to .modelstar/session.yaml
    # TODO: add all the subsequent calls to first call this to check the session name from session.yaml
    # TODO: and then add a function that looks at the session config to load the target set with modelstar use
    config = load_config(target_config)

    # TODO: this should be a actual object and we should extract the .print table from this.
    # as response.prettyprint or just add a __str__ print to this data class for print ithe response.
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
# TODO: ONly accept register_type: function, procedure
@click.argument("register_type", required=True)
@click.argument("file_function_pointer", required=True)
@click.pass_context
def build(ctx, register_type, file_function_pointer):
    '''
    modelstar register <function_name>
        registers the function that is in the functions folder.
        file_path:function_name
    '''

    file_path, file_folder_path, file_name, function_name = strip_file_namespace_pointer(
        file_function_pointer)

    click.echo(
        f"\n\tRegistering {register_type}: `{function_name}` from `{file_name}`\n")

    check_project_structure()

    # TODO: get from session
    # TODO Add stage name here, or use default user stage
    # TODO do the above in the config context
    config = load_config('snowflake')

    if register_type == 'function':
        response = register_function_from_file(
            config, function_name, file_name, file_path)
        click.echo(response)
        click.echo(
            f"\n\tFunction available at: `{config.database}.{config.schema}`\n")

    elif register_type == 'procedure':
        response = register_procedure_from_file(
            config, function_name, file_name, file_path)
        if isinstance(response, list):
            for res in response:
                click.echo(res)
        else:
            click.echo(response)
        click.echo(
            f"\n\tStored Procedure available at: `{config.database}.{config.schema}`\n")

    else:
        raise ValueError(
            f'`{register_type}` not a valid modelstar register command option.')


@main.command("create")
@click.argument("option", required=True)
@click.argument("source_name_pointer", required=True)
@click.pass_context
def build(ctx, option, source_name_pointer):
    '''
    modelstar create <option> <source>
    modelstar create table project/data/abc.csv:table_name
    '''
    click.echo(
        f"\n  Creating `{option}` from `{source_name_pointer}`")

    abs_file_path, file_folder_path, file_name, table_name = strip_file_namespace_pointer(
        source_name_pointer)

    # TODO: get from session
    # TODO Add stage name here, or use default user stage
    # All config options should go into the load_config
    # also pass in the context and set the stage within the load_config
    config = load_config('snowflake')

    if option == 'table':
        create_table(config, file_path=abs_file_path, table_name=table_name)
        # click.echo(
        #     f"\n  Uploading file: `{abs_file_path}`\n")

        # response = upload_file(config, abs_file_path)

        # click.echo(response)

        # click.echo(
        #     f"\n  File available at: `{config.database}.{config.schema}.@{config.stage}`\n")
    else:
        click.echo(f'Create `{option}` is invalid.')
