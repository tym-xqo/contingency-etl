#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from raw import db
from dotenv import load_dotenv

load_dotenv()
SNOWFLAKE_URL = os.getenv("SNOWFLAKE_URL")
POSTGRES_URL = os.getenv("POSTGRES_URL")


def get_previous_timestamps(tbl="enrollments"):
    timestamp_sql = f""" select max(created_at) as max_created
                              , max(updated_at) as max_updated
                              , max(id) max_id
                           from {tbl};
    """
    r = db.result(timestamp_sql)[0]
    timestamps = dict(
        max_created=r["max_created"], max_updated=r["max_updated"], max_id=r["max_id"]
    )
    return timestamps


def get_results(tbl="enrollments"):
    timestamps = get_previous_timestamps()
    max_created = timestamps["max_created"]
    max_updated = timestamps["max_updated"]
    max_id = timestamps["max_id"]
    extract_sql = f""" select *
                         from {tbl}
                        where created_at > :max_created
                           or updated_at > :max_updated
                           or id > :max_id
    """
    rows = db.result(
        extract_sql, max_created=max_created, max_updated=max_updated, max_id=max_id
    )
    return rows
