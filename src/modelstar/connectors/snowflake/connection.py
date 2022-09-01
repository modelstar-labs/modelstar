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
        version = one_row
    except:
        version = 'unknown'
    finally:
        cs.close()
    ctx.close()

    return version