"""Create Stau recurring job queue

"""
import os
from pytz import utc
from datetime import datetime
from copy import deepcopy
import logging as lg
import traceback
import importlib

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from stau_db_interface import get_mysql_config, StauQueue as Stau
from stau_utils import ModuleLoader
from stau_schedule import recurring_jobs

LOG_ap = lg.getLogger("apscheduler")
LOG_ap.setLevel(lg.WARNING)
LOG = lg.getLogger("stau_scheduler")
LOG.setLevel(lg.INFO)


def submit_job(**kwargs):
    """Add job to Stau

    Forward all keyword arguments supplied to the function as arguments for the job in stau.
    The arguments are read from the stau_schedule file.

    Args:
        kwargs (dict): All arguments to forward to the StauJob class
    """
    try:
        # Get connection to queue from the arguments and do not forward it to the add_job function
        with Stau() as queue:
            job_type = kwargs["jobType"]

            # Load the report config section
            module = ModuleLoader(job_type)
            config = module.load_report_config()

            if not config.allow_multiple_jobs and queue.is_job_type_in_queue(job_type):
                # This job type only allows a single job in the queue at once and the queue already contains a job in the
                # QUEUED, PENDING, RUNNING, or MASTER state. Don't submit the current job.
                LOG.info(f"{job_type} wasn't added to the Stau queue since a job of that type already exists")
                return

            queue.add_job(kwargs)
            LOG.info(f"{job_type} was just added to the Stau queue")
    except Exception as e:
        job_type = kwargs.get("jobType", "Unknown")
        LOG.error(f"Unable to submit job {job_type} with arguments {kwargs.keys()}. Error: {e}")
        traceback.print_tb(e.__traceback__)


class StauPeriodicJobsScheduler:
    """Main scheduling class"""

    def __init__(self, start_scheduler=True, submit_jobs_now=False):
        """Construct a new scheduler

        Args:
            start_scheduler (bool):
                Start the scheduler during construction
            submit_jobs_now (bool):
                Schedule jobs but also add them to the queue now
        """
        self.known_job_names = []
        self.scheduler = self.create_scheduler()
        if start_scheduler:
            self.start()

        # Update database with changes to the schedule
        for job in deepcopy(recurring_jobs):
            self.add_or_modify_job(job, submit_jobs_now)

        self.prune_old_jobs()

        # Notify the scheduler that there may be jobs due for execution
        self.scheduler.wakeup()

    def start(self):
        """Start the scheduler"""
        self.scheduler.start()

    def create_scheduler(self):
        """Create a new scheduler object"""
        creds = get_mysql_config()
        connection_string = f"mysql+mysqlconnector://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"

        mysql_jobstore_config = {
            "default": SQLAlchemyJobStore(
                url=connection_string,
                tablename="StauRecurringJobs",
                # Force SQLAlchemy to use pure Python or C-extensions. Removing this options may result
                # in a mixed use of both libraries that handle MYSQL blob storage differently - Resulting
                # in a SystemError from the database.
                # Reference: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
                engine_options={"connect_args": {"use_pure": True}},
            )
        }

        scheduler = BackgroundScheduler(timezone=utc, jobstores=mysql_jobstore_config)

        job_defaults = {
            # Only run one instance of each job
            "coalesce": True,
            "misfire_grace_time": 1800,  # CH55416: Allow jobs to misfire by 30 minutes
        }

        scheduler = BackgroundScheduler(jobstores=mysql_jobstore_config, job_defaults=job_defaults, timezone=utc)
        return scheduler

    def add_or_modify_job(self, job_dict, submit_jobs_now=False):
        """Add a new job or modify an existing scheduled job

        This function sets up the scheduler based on the jobs defined in `stau_scheduler.py`. It processes the list of
        scheduled jobs and ensures they are added to the queue when required by calling the `submit_job` function with
        the expected arguments. If a job with the same name already exists the job is updated otherwise a new job is
        created.

        Args:
            job_dict (dict): Argument to forward to the Stau API
            submit_jobs_now (bool): Schedule jobs but also add them to the queue now

        """
        # Rename dict keys from a user-friendly name to a format the API expects
        if "kwargs_stau_api" in job_dict:
            job_dict["kwargs"] = job_dict.pop("kwargs_stau_api")

        if "kwargs" in job_dict and "kwargs_func" in job_dict["kwargs"]:
            job_dict["kwargs"]["kwargs"] = job_dict["kwargs"].pop("kwargs_func")

        LOG.debug(f"Scheduler is adding job to queue: {job_dict}")

        if submit_jobs_now:
            # In staging we want to test that all jobs work as expected. So they are submitted to follow the normal cron
            # schedule (because of the 'trigger' entry in job_dict) but also added to the queue now (by passing next
            # _run_time to the add_job method). We use utcnow() because APScheduler expects all times in UTC.
            # This is clear when printing the job information using `self.scheduler.print_jobs()`,
            #
            # Quarter-hourly incident battery discharge detection (trigger: cron[hour='*/1'], next run at: 2021-07-01 15:46:59 UTC
            #
            # Also see
            #   tests.test_stau_schedule.test_schedule_submit_all_reports
            self.scheduler.add_job(**job_dict, func=submit_job, replace_existing=True, next_run_time=datetime.utcnow())
        else:
            # Only add jobs to follow the normal cron schedule. Don't also execute them right now.
            self.scheduler.add_job(**job_dict, func=submit_job, replace_existing=True)

        LOG.debug(f"Added or updated information for job {job_dict['name']}")
        self.known_job_names.append(job_dict["name"])

    def prune_old_jobs(self):
        """Remove old jobs

        When an user deletes a job from the list in `stau_schedule`
        that job must also be removed from the database. This function
        checks the added or modified jobs against the database jobs.
        If any jobs only exists in the database they are removed.
        """
        for job in self.scheduler.get_jobs():
            if job.name not in self.known_job_names:
                LOG.info(f"Removing job {job.name}")
                job.remove()


if __name__ == "__main__":
    """Testing this function

    You can test this function by executing it directly from the commandline. It will then connect to your local testing
    database and write the content of stau_scheduler.py to the local table `StauRecurringJobs`. If no executor is
    running (i.e. python src/stau_executor.py) the jobs are simply added to the local queue (StauQueue). To speedup
    testing, increase the scheduling of jobs by increaseing the CronTrigger (e.g. CronTrigger(minute='*/5') ).
    Once the scheduler is running you should see jobs pilling up in the local queue.

    REMEMBER to reload this script every time `stau_scheduler.py` is changed.
    """
    scheduler = StauPeriodicJobsScheduler()
    while True:
        pass
