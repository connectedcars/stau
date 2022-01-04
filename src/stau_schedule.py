"""List of recurring jobs

To schedule a new job for periodic/recurring execution in Stau add it
to the list below. Each job is a dict with the following parameters,

* id (str): User-defined unique identifier
* name (str): User-defined descriptive name
* trigger (APScheduler trigger): When the job should run
* kwargs (dict): Keyword arguments forwarded to the Stau API
  - jobType (str): The name of the python script to execute
  - kwargs (dict): Any arguments to provide the main function
  - dependencies (list or None): If None the job will resolve its own dependencies
                                 otherwise a list of dependent jobIds is supplied.
"""
# from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

############################################
# ADD RECURRING JOB DEFINITIONS BELOW
#
# https://apscheduler.readthedocs.io/en/latest/modules/schedulers/base.html#apscheduler.schedulers.base.BaseScheduler.add_job
############################################

# fmt: off
# Note "work_ids": [0] The report does not run on individual work_ids. Work_id = 0 is a dummy placeholder for all such reports.
# fmt: off
recurring_jobs = [

    {
        "id": "Say hello",
        "name": "Say hello",
        "trigger": CronTrigger(minute='*/20'),  # Run every hour. IMPORTANT: Update kwargs if changed
        "kwargs_stau_api": {
            "jobType": "job_hello_work",
        }
    },

    {
        "id": "Say goodbye",
        "name": "Say goodbye",
        "trigger": CronTrigger(minute='*/1'),  # Run every hour. IMPORTANT: Update kwargs if changed
        "kwargs_stau_api": {
            "jobType": "job_goodbye_work",
            "kwargs_func": {
                "final_message": "Thanks for trying Stau!"
            },
        }
    },

]
# fmt: on
