#!/usr/bin/env python
import logging as lg

import stau_utils as utils

# CONFIG
NAME = "job_goodbye_work"
LOG = lg.getLogger(NAME)
CHUNKSIZE = 2

STAU_CONFIG = utils.ReportConfig(
    job_type=NAME,
    chunk_size=CHUNKSIZE,
    dependencies=[
        # This job depends on having said hello first
        utils.ReportDependencies(
            "job_hello_work", # Name of job to call
            0 # job must have executed 0s ago - i.e. always execute the dependency
        ),
    ],
    main_func=f"{NAME}_func",
)

def select_work(**kwargs):
    """Get work IDs to run the Goodbye job against"""
    return [0, 1]


def job_goodbye_work_func(work_ids=None, final_message=None, **kwargs):
    """This is the main entry point for the Goodbye job"""
    if work_ids is None:
        # Default to run against all work IDs
        work_ids = select_work()

    for wid in work_ids:
        LOG.warning(f"Goodbye work ID {wid}")

    if final_message is not None:
        LOG.warning(final_message)


if __name__ == "__main__":
    # All jobs in Stua simply run through the main function this file can execute locally without issue
    job_goodbye_work_func()
