import click
from modelstar import logger, session_registry
from modelstar.commands.project import initialize_project
from modelstar.commands.database import list_databases
from modelstar.commands.register import register_function_from_file, register_procedure_from_file
from modelstar.commands.upload import upload_file
from modelstar.commands.create import create_table
from modelstar.commands.run import run_sql
from modelstar.commands.download import view_download_records, build_new_report
from modelstar.executors.config import set_session, load_config
from modelstar.executors.project import check_project_folder_structure
from modelstar.utils.path import strip_file_namespace_pointer, check_file_path, map_ml_builtins


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
@click.argument("project_name", required=True, type=str)
@click.pass_context
def init(ctx, project_name: str):
    '''
    modelstar init <project_name>/<. = current folder name>
        Creates a folder with the project name using a project template    
    '''

    project_path = initialize_project(project_name=project_name)

    logger.echo('Your project has been created.')
    logger.echo('Project location', detail=project_path)


@main.command("use")
@click.argument("target_config", required=True)
@click.pass_context
def session(ctx, target_config):
    '''
    modelstar use <target_config>
        Checks if the config.modelstar parameters are right
        Connects to the server of snowflake and gets all the database info 
    '''

    check_project_folder_structure()

    logger.echo('Setting session with configuration', detail=target_config)

    set_session(target_config)

    # TODO: Pass in the context to set the optional stage, db, warehouse and all in runtime.
    config = load_config()
    logger.echo('Loaded session', detail=config.name)

    response = list_databases(config)

    logger.echo('Showing available databases for config', detail=config.name)
    logger.echo(response)
    logger.echo(' ')


@main.command("upload")
@click.argument("file_path", required=True)
@click.pass_context
def build(ctx, file_path):
    '''
    modelstar upload <file_path>
        Uploads the file from <file_path> into the cloud location.
    '''

    logger.echo('Checking file', detail=file_path)

    abs_file_path = check_file_path(file_path)

    logger.echo('Uploading file', detail=abs_file_path)

    check_project_folder_structure()
    config = load_config()

    response = upload_file(config, file_path)

    logger.echo(response)

    logger.echo('File available at',
                detail=f'{config.database}.{config.schema}.@{config.stage}')


@main.command("register")
# TODO: Only accept register_type: function, procedure
@click.argument("register_pointer", required=True)
@click.argument("file_function_pointer", required=False)
@click.pass_context
def build(ctx, register_pointer, file_function_pointer):
    '''
    modelstar register <function_name>
        registers the function that is in the functions folder.
        file_path:function_name
    '''

    session_registry.load_registry()
    check_project_folder_structure()
    config = load_config()

    if (register_pointer == 'function' or register_pointer == 'procedure'):
        if file_function_pointer is None:
            raise ValueError(f'Provide a `file_function_pointer` of the `{register_pointer}` to register.')
        file_path, file_folder_path, file_name, function_name = strip_file_namespace_pointer(
            file_function_pointer)
        register_type = register_pointer
        logger.echo(
            f"Registering `{register_type}` as `{function_name}` from `{file_name}`")
    else:
        file_path, file_folder_path, file_name, function_name, register_type = map_ml_builtins(
            builtin_pointer=register_pointer)
        logger.echo(f"Registering `{function_name}` into {config.database}.{config.schema} of your data warehouse.")

    version = 'V1'

    # print(file_path, file_folder_path, file_name, function_name, register_type, version)

    if register_type == 'function':
        response = register_function_from_file(
            config, function_name, file_name, file_path, version=version)
        logger.echo(response)
        # TODO: Add this info to the response
        logger.echo("Function available at",
                    detail=f"`{config.database}.{config.schema}`")

    elif register_type == 'procedure':
        response = register_procedure_from_file(
            config, function_name, file_name, file_path, version=version)
        logger.echo(response)
        # TODO: Add this info to the response
        logger.echo("Stored Procedure available at",
                    detail=f"`{config.database}.{config.schema}`")

    else:
        # TODO Make custom error for these.
        raise ValueError(
            f'`{register_type}` not a valid modelstar register command option.')

    session_registry.add_register(
        name=function_name, type=register_type, version=version)
    session_registry.dump_registry()


@main.command("deploy")
# TODO: Only accept register_type: function, procedure
@click.argument("builtin_pointer", required=True)
@click.pass_context
def build(ctx, builtin_pointer):
    '''
    modelstar register <function_name>
        registers the function that is in the functions folder.
        file_path:function_name
    '''

    session_registry.load_registry()

    function_name, file_name, file_path = map_ml_builtins(
        builtin_pointer=builtin_pointer)

    version = 'V1'

    logger.echo(
        f"Registering {register_type} as `{function_name}` from `{file_name}`")

    check_project_folder_structure()
    config = load_config()

    # if register_type == 'function':
    #     response = register_function_from_file(
    #         config, function_name, file_name, file_path, version=version)
    #     logger.echo(response)
    #     # TODO: Add this info to the response
    #     logger.echo("Function available at",
    #                 detail=f"`{config.database}.{config.schema}`")

    # elif register_type == 'procedure':
    #     response = register_procedure_from_file(
    #         config, function_name, file_name, file_path, version=version)
    #     logger.echo(response)
    #     # TODO: Add this info to the response
    #     logger.echo("Stored Procedure available at",
    #                 detail=f"`{config.database}.{config.schema}`")

    # else:
    #     # TODO Make custom error for these.
    #     raise ValueError(
    #         f'`{register_type}` not a valid modelstar register command option.')

    # session_registry.add_register(
    #     name=function_name, type=register_type, version=version)
    # session_registry.dump_registry()


@main.command("create")
@click.argument("option", required=True)
@click.argument("source_name_pointer", required=True)
@click.pass_context
def build(ctx, option, source_name_pointer):
    '''
    modelstar create <option> <source>
    modelstar create table project/data/abc.csv:table_name
    '''

    abs_file_path, file_folder_path, file_name, table_name = strip_file_namespace_pointer(
        source_name_pointer)

    logger.echo(f"Creating `{option}` as `{table_name}` from `{file_name}`")

    check_project_folder_structure()
    config = load_config()

    if option == 'table':
        # TODO: Migrate to https://docs.snowflake.com/en/user-guide/python-connector-api.html#write_pandas
        response = create_table(
            config, file_path=abs_file_path, table_name=table_name)

        logger.echo(response)

        logger.echo(f"Table: {table_name} available at",
                    detail=f"`{config.database}.{config.schema}`")
    else:
        # TODO Make custom error for these.
        raise ValueError(f'`Create `{option}` is invalid.')


@main.command("run")
@click.argument("sql", required=True)
@click.pass_context
def build(ctx, sql):
    '''
    modelstar create <option> <source>
    modelstar create table project/data/abc.csv:table_name
    '''

    check_project_folder_structure()
    config = load_config()

    logger.echo('Loaded session', detail=config.name)

    logger.echo('Running SQL')

    response = run_sql(config, sql=sql)

    logger.echo(response)
    logger.echo(' ')


@main.command("check")
@click.argument("run_id", required=True)
@click.pass_context
def build(ctx, run_id):
    '''
    modelstar check     
    '''

    session_registry.load_registry()

    check_project_folder_structure()
    config = load_config()

    logger.echo('Loaded session', detail=config.name)

    # build_new_report(run_id)

    view_download_records(config=config, run_id=run_id)
    session_registry.dump_registry()
    logger.echo(' ')
