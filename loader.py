#!/usr/bin/env python
# -*- coding: utf-8 -*-
from etl import log, pg_source
from etl.snowflake_target import snowflake_target


def thingo(tbl="enrollments"):
    rows = pg_source.get_results(tbl=tbl)
    target_table = "test_enrollments"
    snowflake_target(
        namespace="pc_fivetran_db.manual_wmx_api_stopgap",
        table=target_table,
        rows=rows,
    )
    log.info(f" data loaded to {target_table}")
    return


if __name__ == "__main__":
    thingo(tbl="activities")
