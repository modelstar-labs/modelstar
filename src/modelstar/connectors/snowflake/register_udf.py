import snowflake.connector


def register_udf_from_file(config, file_path: str, func_name: str):
    ctx = snowflake.connector.connect(
        user=config['username'],
        password=config['password'],
        account=config['account']
    )
    cs = ctx.cursor()
    try:
        database = config['database']
        schema = config['schema']
        stage = 'test_1'
        cs.execute(f"use {database}.{schema}")
        cs.execute(f"put file://{file_path} @{stage}")
        cs.execute(f"""create or replace function {func_name}(a int, b int)
returns int
language python
runtime_version = '3.8'
handler = 'addition.addition'
imports = ('@test_1/addition.py')""")
    except Exception as e:
        print(e.args)
    finally:
        cs.close()
    ctx.close()
