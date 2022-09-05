import snowflake.connector


def put_file(config, file_path: str):
    ctx = snowflake.connector.connect(
        user=config['username'],
        password=config['password'],
        account=config['account']
    )
    cs = ctx.cursor()
    try:
        cs.execute("use TEST_ML_1_KAGGLE.public")
        cs.execute(f"put file://{file_path} @test_1")
    except Exception as e:
        print(e.args)
    finally:
        cs.close()
    ctx.close()
