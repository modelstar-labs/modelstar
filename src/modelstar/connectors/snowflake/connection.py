#!/usr/bin/env python
import snowflake.connector


def test(config):
    # Gets the version
    ctx = snowflake.connector.connect(
        user=config['username'],
        password=config['password'],
        account=config['account']
        )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()