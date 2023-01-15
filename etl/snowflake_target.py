#!/usr/bin/env python
# -*- coding: utf-8 -*-
from raw import db
import csv
import tempfile

from etl import log

DEFAULT_NAMESPACE = "pc_fivetran_db.manual_wmx_api_stopgap"


def snowflake_target(
    table, namespace=DEFAULT_NAMESPACE, rows=[{}], stagepath="", truncate=False
):
    """Given a set of `rows` (list of dictionaries),
    upload as csv to table stage and bulk load to `table`"""
    # TODO: Add step to insert row in marker table when target load is complete

    # prevent doubling of % in table stage name by Snowflake dialect parser
    db.engine(paramstyle="named")

    # define stage name for table and append path
    target_stage = f"@{namespace}.%{table}"
    if stagepath:
        target_stage += f"/{stagepath}"

    # write rows to CSV in tempfile
    with tempfile.NamedTemporaryFile(mode="wt+", delete=False) as f:
        fieldnames = [key.lower() for key in rows[0].keys()]
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(row)
        f.flush()
        put_sql = f"put file://{f.name} {target_stage}"
        db.result(put_sql)
        log.info(f"CSV uploaded to {target_stage}")

    # truncate target table before copying from stage if set
    if truncate:
        truncate_sql = f"truncate table {namespace}.{table}"
        db.result(truncate_sql, autocommit=True)

    # copy data from stage into target table
    copy_sql = (
        f"copy into {namespace}.{table} from {target_stage} "
        "file_format = (type = CSV) purge = true"
    )
    db.result(copy_sql)
    log.info(f"{namespace}.{table} copied from {target_stage}")
