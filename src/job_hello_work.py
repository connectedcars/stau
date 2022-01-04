#!/usr/bin/env python
import logging as lg

import stau_utils as utils

# CONFIG
NAME = "job_hello_work"
LOG = lg.getLogger(NAME)
CHUNKSIZE = 1

STAU_CONFIG = utils.ReportConfig(
    job_type=NAME,
    chunk_size=CHUNKSIZE,
    dependencies=[], # No dependencies
    main_func=f"{NAME}_func",
)

def select_work(**kwargs):
    """Get work IDs to run the Hello job against"""
    return [0]


def job_hello_work_func(work_ids=None, **kwargs):
    """This is the main entry point for the Hello job"""
    if work_ids is None:
        # Default to run against all work IDs
        work_ids = select_work()

    for wid in work_ids:
        LOG.warning(f"Hello work ID {wid}")

if __name__ == "__main__":
    # All jobs in Stua simply run through the main function this file can execute locally without issue
    job_hello_work_func()
