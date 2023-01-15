#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Log progress messages to stdout
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_URL = os.getenv("SNOWFLAKE_URL")
POSTGRES_URL = os.getenv("POSTGRES_URL")


logging.basicConfig(
    format="{asctime} [{levelname}] {message}",
    level=logging.INFO,
    stream=sys.stdout,
    style="{",
)

# Supress INFO logs from imported libraries
logging.getLogger("snowflake").setLevel(logging.CRITICAL)

log = logging.getLogger()
