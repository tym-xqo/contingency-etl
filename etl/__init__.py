#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Log progress messages to stdout
import logging
import sys


logging.basicConfig(
    format="{asctime} [{levelname}] {message}",
    level=logging.INFO,
    stream=sys.stdout,
    style="{",
)

# Supress INFO logs from imported libraries
logging.getLogger("snowflake").setLevel(logging.CRITICAL)

log = logging.getLogger()
