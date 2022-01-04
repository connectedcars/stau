#!/usr/bin/env python
"""Stau - Data science work queue

Stau is a task queue designed for multi-type dependent jobs build around a central database as backend storage.

This file holds all the business logic that validates the database integrity is intact and interacts with the
backend storage system.
"""
from datetime import datetime
import time
import json
import os
import random
from copy import deepcopy
import logging as lg

import mysql.connector

# Setup global logger
LOG = lg.getLogger("stau_db_interface")

# CONSTANTS
QUEUE_TABLE = "StauQueue"
EXECUTE_TABLE = "StauLatestExecution"
MYSQL_RETRY_WAIT_TIME = 30  # Seconds
MYSQL_MAX_RETRIES = 10  # Number of retries on failed database queries before failing the whole query

# Possible job states. See WIKI for information on each state.
JOBSTATUS = ["QUEUED", "PENDING", "RUNNING", "DONE", "MASTER", "CANCELED", "FAILED", "LOCKED"]


class InvalidStatusChangeError(Exception):
    """Non-valid change in job status

    This error is raised when the user has requested an
    invalid change in a job status.
    """

    pass


def get_mysql_config():
    """Return the the parsed connection string as dict. I.e. can be used in MySQLConnectionPool as:

    mysql_config = dblib.get_mysql_config('DATA_QUALITY')
    mysql_pool = MySQLConnectionPool(pool_size=1, **mysql_config)
    """

    # Return a configuration dict configurations
    return {
        "host": os.environ.get("MYSQL_HOST"),
        "port": os.environ.get("MYSQL_PORT", 3306),
        "user": os.environ.get("MYSQL_USER"),
        "password": os.environ.get("MYSQL_PASSWORD"),
        "database": os.environ.get("MYSQL_DATABASE")
    }


class StauQueue:
    def __init__(self, db_conn=None, local_execution=False):
        """Construct a Stau queue object

        This object is responsible for all communication with the database tables.

        Args:
            db_conn (connection object): Existing database connection object from mysql.connector
            local_execution (bool): Connect to the local database
        """
        self.db_conn = db_conn
        if not db_conn:
            self.db_creds = get_mysql_config()
            self.db_conn = mysql.connector.connect(**self.db_creds)
            self.db_conn.ping()

    def _exec(self, query, params, retries=0):
        """Execute query against the database

        This function contains all logic to communicate with the database.
        It handles type conversion of JSON objects, retry logic and error
        handling in case of poor queries.

        Args:
            query (str):   SQL query to execute against the database
            params (dict): Parameters used to interpolate into the query
            retries (int): Number of connection attempts if database connection fails

        Returns:
            result (list, dict or int): List in case multiple rows are returned. A single row will return a dict. If the SELECT query
                                        did not return any rows an empty list is returned. For INSERT operations
                                        the last affected row id is returned and if no rows were affected -1 is returned.
        """
        if retries == 0:
            # Convert selected dict and list values in the input to JSON so MySQL is able to process them but only
            # on the first try
            params = self._dicts_to_json(params)

        cursor = None
        try:
            cursor = self.db_conn.cursor(dictionary=True)

            if isinstance(params, dict):
                cursor.execute(query, params)
            else:
                cursor.executemany(query, params)

            if "SELECT" not in query:
                # Commit changes to rows
                LOG.debug("Committing changes to database")
                self.db_conn.commit()

                if cursor.rowcount < 1:
                    result = -1
                else:
                    result = cursor.lastrowid
            else:
                LOG.debug(f"The query returned {cursor.rowcount} rows")
                if cursor.rowcount < 0:
                    raise RuntimeError("Database result not ready but tries to access it anyways")

                result = cursor.fetchall()
                if len(result) == 1:
                    # Only return a single row
                    result = result[0]

        except mysql.connector.errors.OperationalError as db_err:
            # Retry on operational errors. This exception is raised for errors which are related to MySQL's operations.
            # For example: too many connections; a host name could not be resolved; bad handshake; server is shutting
            # down, communication errors.
            # All possible exceptions: https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors.html
            if retries < MYSQL_MAX_RETRIES:
                LOG.info(
                    f"Encountered database problems on worker {os.environ.get('HOSTNAME', 'Unknown')} - Retry {retries} of {MYSQL_MAX_RETRIES} in {MYSQL_RETRY_WAIT_TIME}s"
                )
                time.sleep(MYSQL_RETRY_WAIT_TIME)
                if cursor is not None:
                    # Close any existing cursors (there might not be any) before creating a new
                    cursor.close()

                result = self._exec(query, params, retries=retries + 1)
            else:
                # Raise error up the stack if retrying the connection didn't solve the problem
                raise IOError(
                    f"Unable to access database from worker {os.environ.get('HOSTNAME', 'Unknown')} after {MYSQL_MAX_RETRIES} tries"
                ) from db_err
        except (mysql.connector.errors.InternalError, mysql.connector.errors.DatabaseError) as db_err:
            # Retry on Database or InternalErrors. This normally indicates stuck commands or deadlocks between
            # concurrent executors. The retry interval here is shorter as concurrency errors simply needs to retry
            # to avoid the locks.
            if retries < MYSQL_MAX_RETRIES:
                LOG.info(
                    f"Encountered conflicting database state on query {query} - Retry {retries} of {MYSQL_MAX_RETRIES} in between 1-10s"
                )
                # Close cursor to get a fresh transaction
                self.db_conn.rollback()
                if cursor is not None:
                    # Close any existing cursors (there might not be any) before creating a new
                    cursor.close()

                # Sleep between 1 - 10 seconds and try again - This might help solve the deadlock
                time.sleep(random.randint(1, 10))

                # Try again
                result = self._exec(query, params, retries=retries + 1)
            else:
                # Raise error up the stack if retrying the connection didn't solve the problem
                raise IOError(f"Unable to execute {query} after {MYSQL_MAX_RETRIES} tries") from db_err
        else:
            # Only convert the database response if no exception was raised
            result = self._dicts_to_json(result)
        finally:
            try:
                if cursor is not None:
                    # Close only the cursor (the local buffer of data with the database). The connection is not closed here
                    # since that would release any locks in place by the process. It is left to the caller to decide if the
                    # connection is closed after this call terminates by calling self.db_conn.close()
                    cursor.close()
            except Exception as err:
                LOG.exception(
                    f"Unable to close database cursor after executing command {query[:128]} - will try to continue.",
                    exc_info=err,
                )

        return result

    def _dicts_to_json(self, din):
        """Convert job dict to or from MySQL compatible format

        The parameters (kwargs, history and dependencies) are MySQL JSON objects. In Python the native type is dict.
        This function converts the parameters to JSON if they are dicts and to dicts if they are JSON encoded strings.

        Args:
            din (list or dict): Values to convert. If called with a list each element is converted.

        Returns:
            out (same shape as input): The input converted to or from the database format

        Raises:
            TypeError: If the input does not contain the expected parameters
        """
        LOG.debug(f"Converting input {din}")
        out = deepcopy(din)

        try:
            if isinstance(out, list):
                for itt in range(len(out)):
                    # Recursive call to transform each object in the list
                    out[itt] = self._dicts_to_json(out[itt])
            elif isinstance(out, dict):
                if (
                    "kwargs" in out
                    and isinstance(out["kwargs"], str)
                    or "dependencies" in out
                    and isinstance(out["dependencies"], str)
                    or "history" in out
                    and isinstance(out["history"], str)
                ):
                    # Convert from encoded JSON to dict
                    LOG.debug("Convert from JSON")
                    if "kwargs" in out:
                        out["kwargs"] = json.loads(out["kwargs"])
                    if "history" in out:
                        out["history"] = json.loads(out["history"])
                    if "dependencies" in out:
                        out["dependencies"] = json.loads(out["dependencies"])
                else:
                    LOG.debug("Convert to JSON")
                    # Convert from dict to encoded JSON
                    if "kwargs" in out:
                        out["kwargs"] = json.dumps(out.get("kwargs")) if out["kwargs"] is not None else "{}"
                    if "dependencies" in out:
                        out["dependencies"] = (
                            json.dumps(out.get("dependencies")) if out["dependencies"] is not None else "null"
                        )
                    if "history" in out:
                        out["history"] = json.dumps(out.get("history")) if out["history"] is not None else "null"
        except KeyError:
            raise TypeError(f"Unable to convert input to or from MySQL format due to incorrect format. Input was {din}")
        return out

    def add_job(self, job):
        """Add job to queue

        The expected format of job is a dict with the following keys:
          - jobType: "report_dataX"
          - kwargs: {"work_ids": [1, 2, 3]}
          - dependencies: [42, 43]
          - masterJobId: e.g. 42

        Args:
            job (dict): Job to add to queue

        Returns:
            JobId (int): Unique ID for the new job
        """
        # Set default values for job arguments if the user didn't provide them
        job["dependencies"] = job.get("dependencies", None)
        job["kwargs"] = job.get("kwargs", {})

        # Set master job id to None if no id is supplied
        job["masterJobId"] = job.pop("masterJobId", None)
        query = f"""
          INSERT INTO {QUEUE_TABLE}
          (jobType, kwargs, status, dependencies, masterJobId)
          VALUES
          (%(jobType)s, %(kwargs)s, 'QUEUED', %(dependencies)s, %(masterJobId)s)
        """
        job_id = self._exec(query, job)
        LOG.debug(f"Added a new job with id {job_id} and params: {job}")
        return job_id

    def get_job(self, job_id):
        """Get a specific job by job id

        Args:
            job_id (int): Unique id of the job
            for_update (bool): True if you will update the job soon

        Returns:
            job (dict): Job parameters for the job id
        """
        query = f"SELECT * FROM {QUEUE_TABLE} WHERE id=%(job_id)s"
        params = {"job_id": job_id}
        jobs = self._exec(query, params)
        return jobs

    def get_job_to_run(self, exec_env=""):
        """Get a job id to execute

        Due to the parallel structure of Stau, jobs must start in a single database
        transaction. Otherwise a different worker may alter the job state to ensure
        consistency. This function selects a PENDING job and moves it to RUNNING. Since
        the SELECT and UPDATE happens within a single transaction it ensures only a
        single executor is allowed to run the same job.

        Args:
            exec_env (str): Name or SHA of the current execution environment
        """
        try:
            cursor = self.db_conn.cursor(dictionary=True)
            query = f"""
              SELECT id
              FROM {QUEUE_TABLE}
              WHERE status = 'PENDING'
              ORDER BY updatedAt ASC
              LIMIT 1
              FOR UPDATE
            """
            cursor.execute(query)
            LOG.debug(f"The query returned {cursor.rowcount} rows")
            job = cursor.fetchone()
            if job is None:
                # There was no work to get
                return None

            # Get lock - wait max 5 seconds before failing the GET_LOCK operation
            query_lock = """
            DO GET_LOCK(CONCAT('StauJob', %(job_id)s), 5)
            """
            cursor.execute(query_lock, {"job_id": job["id"]})

            query2 = f"""
              UPDATE {QUEUE_TABLE}
              SET status = 'RUNNING', execEnvironment = %(execEnv)s, execStart = NOW()
              WHERE id = %(job_id)s
              AND IS_USED_LOCK(CONCAT('StauJob', %(job_id)s)) = CONNECTION_ID()
            """
            cursor.execute(query2, {"job_id": job["id"], "execEnv": exec_env})

            self.db_conn.commit()
        except Exception as err:
            self.db_conn.rollback()
            LOG.error(f"Failed to get job with error {err}")
            result = None
        else:
            result = self.get_job(job["id"])

        return result

    def get_jobs(self, status, max_age_seconds=604800):
        """Get list of all jobs in database with specific status

        Args:
            status (str): Get jobs with this status
            max_age_seconds (int): Only return jobs updated within the last N seconds. Default: 7 days

        Returns:
            jobs (list): List of jobs encoded as dict
        """
        status = status.upper()
        if status not in JOBSTATUS:
            raise ValueError(f"Cannot lookup status of value {status} - must be one of {JOBSTATUS}")
        query = f"""
            SELECT *
            FROM {QUEUE_TABLE}
            WHERE status = %(status)s
            AND updatedAt >= NOW() - INTERVAL {max_age_seconds} SECOND
        """
        params = {"status": status, "max_age_seconds": max_age_seconds}
        jobs = self._exec(query, params)

        if isinstance(jobs, dict):
            # Always returns list even for a single job
            jobs = [jobs]

        return jobs

    def is_job_type_in_queue(self, job_type):
        """Check if a specific job type is already waiting to finish

        This check is useful to know if a job with the same type is active in the queue. E.g. such a check
        is used to ensure a job isn't added to the queue if an identical job is already waiting in the queue.

        Args:
            job_type (str): Type of job which is normally the same as the script name. E.g: `report_service`

        Returns:
            waiting (bool): Returns True if a job of this type is QUEUED, PENDING, RUNNING, or MASTER.
        """
        job_type = job_type.lower()
        query = f"""
        SELECT id
        FROM {QUEUE_TABLE}
        WHERE jobType = %(job_type)s
        AND status IN ('QUEUED', 'PENDING', 'RUNNING', 'MASTER')
        LIMIT 1
        """
        params = {"job_type": job_type}
        jobs = self._exec(query, params)

        if any(jobs):
            return True
        else:
            return False

    def iter_jobs(self, status):
        """Iterate through the queue one job at a time

        Similar to get_jobs but only returns a single job at a time as
        an iterator.

        This function is used when the executors are looking for work. The
        get_jobs method returns all jobs of a given status. That is not always an optimal
        pattern as the list quickly gets outdated and executors therefore may act
        based on outdated data.

        IMPORTANT: Although the SQL statement uses locks multi-transaction concurrency does not ensure the job is not
        modified before it is returned. This function is therefore not safe for multi-transaction scenarios.

        Args:
            status (str): Get jobs in a specific status

        Returns:
            job (iterator): A single random job with that status

        Raises:
            StopIteration: If called with the next operator and there is no work on the queue
        """
        status = status.upper()
        if status not in JOBSTATUS:
            raise ValueError(f"Cannot lookup status of value {status} - must be one of {JOBSTATUS}")

        # Get the oldest job with a specific status from the queue. To ensure the iterator does not
        # create an infinite loop each job is only examined once by supplying the known_ids variable to
        # the query.
        # NOTE: Use the SKIP_LOCK syntax in the query below once we upgrade to MySQL 8.x to ensure multi-transaction support
        query = f"""
          SELECT *, GET_LOCK(CONCAT('StauJob', id), 5) AS lockStatus
          FROM {QUEUE_TABLE}
          WHERE status = %(status)s
          AND NOT JSON_CONTAINS(%(knownIds)s, JSON_ARRAY(id))
          AND IS_FREE_LOCK(CONCAT('StauJob', id)) = 1
          ORDER BY updatedAt ASC
          LIMIT 1
        """
        known_ids = []
        params = {"status": status, "knownIds": json.dumps([-1])}
        while job := self._exec(query, params):
            if len(job) == 0:
                # If job is an empty list no more jobs are in that
                # status or they have all been examined once
                break

            job_id = job["id"]
            if job["lockStatus"] != 1:
                LOG.error(
                    f"Executor received a job but a lock was in place with status {job['lockStatus']} - Skipping job"
                )
            else:
                # The job is locked and ready for processing by an external process
                yield job
                # Once the external processing is complete the job is released again
                self.unlock_job(job_id)

            # Hide the job on the next iteration of the queue
            known_ids.append(job_id)
            params["knownIds"] = json.dumps(known_ids)

    def _release_lock(self, lock_name):
        """Release a named lock from the database

        Please use the unlock_job method to release a lock.

        Args:
            lock_name (str): User-defined name for the lock
        """
        query = "SELECT IS_USED_LOCK(%(lockName)s) = CONNECTION_ID() as lockStatus"
        params = {"lockName": lock_name}
        lock_status = self._exec(query, params)
        if lock_status.get("lockStatus", -100) != 1:
            LOG.error("An executor tried to release a lock it did not own")
            return

        query = "SELECT RELEASE_LOCK(%(lockName)s) as lockStatus"
        lock_status = self._exec(query, {"lockName": lock_name})
        if lock_status["lockStatus"] == 0:
            LOG.error("An executor tried to release a lock it did not own")
        elif lock_status["lockStatus"] is None:
            LOG.info("An executor tried to release a non-existing lock")
        elif lock_status["lockStatus"] != 1:
            LOG.error(f"Unable to release the lock '{lock_name}' - lock status was '{lock_status}'")
        else:
            LOG.debug(f"Lock '{lock_name}' was released")

    def _acquire_lock(self, lock_name):
        """Acquire a named database lock

        The lock is held until the process releases the lock or it terminates.

        Please use the lock_job method to create a new lock.

        Args:
            lock_name (str): User-defined name for the lock

        Returns:
            lock_status (bool): True if this process has the lock. Otherwise False.
        """
        query = "SELECT IS_USED_LOCK(%(lockName)s) = CONNECTION_ID() as lockStatus"
        params = {"lockName": lock_name}
        lock_status = self._exec(query, params)
        lock_status = lock_status.get("lockStatus", -100)

        if lock_status is None:
            # No lock - let's get it
            LOG.info(f"Lock '{lock_name}' is available, will acquire it")
            lock_status = self._exec("SELECT GET_LOCK(%(lockName)s, 5) as myLock", params)
            if lock_status["myLock"] != 1:
                LOG.warning(f"Unable to acquire the lock '{lock_name}' that was free")
                return False
            return True
        elif lock_status == 0:
            # Someone else has the lock - Let's bail
            LOG.debug(f"The named lock '{lock_name}' is held by a different process")
            return False
        elif lock_status == 1:
            # I already have the lock - Let's boogie
            LOG.debug(f"The named lock '{lock_name}' is already held by me")
            return True
        else:
            LOG.error(
                f"Unknown lock status '{lock_status}' - The parameter lockStatus was not returned in the lock request"
            )
            return False

    def lock_job(self, job_id):
        """Lock a job

        This prevents other jobs from executing on the same job

        Args:
            job_id (int): Unique job identifier

        Returns:
            lock_status (bool): True if the job was locked otherwise False
        """
        return self._acquire_lock(lock_name=f"StauJob{job_id}")

    def unlock_job(self, job_id):
        """Unlock a job

        Indicate the job is available for other executors

        Args:
            job_id (int): Unique job identifier
        """
        self._release_lock(lock_name=f"StauJob{job_id}")

    def touch_job(self, job_id):
        """Update the updatedAt parameter in the database

        This function is similar to the POSIX touch function. It simply
        changes the updatedAt database parameter to the current timestamp.
        This allows the executors to indicate that they are still executing
        a job and have not died or completed without updating the database.

        Args:
            job_id (int): Job id to update

        Returns:
           row (int): Database row id of the job. This is equal to the job id.
        """
        query = f"UPDATE {QUEUE_TABLE} SET updatedAt = NOW() WHERE id = %(job_id)s"
        params = {"job_id": job_id}
        row = self._exec(query, params)
        if row == -1:
            LOG.warning(
                f"Touching job {job_id} affected zero rows - Either the job does not exist or updatedAt was up to date"
            )
        return row

    def update_job(self, job_id, updated_values, rerun=False):
        """Update job information

        During the entire execution flow the job specific information may
        be updated by the user or the executor.

        Args:
            jobs (iterable of int or int): Job ids to update
            updated_values (iterable of dict or dict): New values to update with
            rerun (bool): If the requested job should be run again

        Returns:
            job (dict): The job parameters after the update

        Raises:
            KeyError: If the requested job does not exist
            AttributeError: If a value in updated_values does not exist
                            on the Job class
            ValueError: If the job changes to a lower status. Ex. RUNNING -> QUEUED
            InvalidStatusChangeError: If updating the job would leave the queue in a conflicting state
        """
        LOG.info(f"Updating job {job_id} with values {updated_values}")
        processing_start = time.time()
        if updated_values is None or updated_values == {}:
            # Handle empty input
            return False

        # Check the object does exist
        job_to_update = self.get_job(job_id)
        if job_to_update is None:
            raise KeyError(f"The job {job_id} does not exist")

        cur_status = job_to_update["status"]
        # Handle rerunning a failed or canceled job
        if rerun and (cur_status == "RUNNING" or cur_status == "CANCELED" or cur_status == "FAILED"):
            # Set status to lowest level
            updated_values["status"] = JOBSTATUS[0]
            # Reset executions
            updated_values["execStart"] = None
            updated_values["execEnd"] = None
        elif rerun:
            raise ValueError("Only running, failed or canceled job can be re-run")

        # Disallow moving jobs "backward" in status
        # re-runs are handled above
        if not rerun and "status" in updated_values:
            status_diff = JOBSTATUS.index(updated_values["status"]) - JOBSTATUS.index(job_to_update["status"])

            if updated_values["status"] == "CANCELED" or updated_values["status"] == "FAILED":
                # Moving jobs to a failed state is always allowed
                pass
            elif updated_values["status"] == "MASTER" or cur_status == "MASTER":
                # Allow jobs to move directly to MASTER
                # And directly to DONE
                pass
            elif cur_status == "RUNNING" and updated_values["status"] == cur_status:
                raise InvalidStatusChangeError("The job is already running")
            elif cur_status == "LOCKED" and updated_values["status"] == cur_status:
                # The request would like to set LOCKED on a job already in that status
                raise InvalidStatusChangeError("Job is already locked")
            elif updated_values["status"] == "LOCKED":
                # Locked jobs can move to any status but cannot update any
                # other values in the process
                updated_values = {"status": updated_values["status"]}
            elif cur_status == "LOCKED":
                # Jobs in the locked state can move to any state
                # This is similar to the properties for failed and canceled
                pass
            elif status_diff != 1:
                # status should either increase by one or be the same
                raise InvalidStatusChangeError(
                    f"Requested status change from {job_to_update['status']} to {updated_values['status']} for job {job_id}. Jobs can only move one status forward at a time."
                )

        # Update execution times when job status changes
        if "status" in updated_values:
            if updated_values["status"] == "PENDING":
                try:
                    job_to_update["kwargs"]["work_ids"]
                except KeyError:
                    raise ValueError(
                        f"Cannot move the job (id: {job_to_update['id']} to pending. No work ids found in the jobs kwargs. Found {job_to_update['kwargs']}"
                    )

            if updated_values["status"] == "RUNNING":
                updated_values["execStart"] = datetime.utcnow()

                # Also move master job if not already started
                pid = job_to_update["masterJobId"]
                if pid is not None:
                    # This is not a master job so record the execution start
                    # on the master job
                    query = f"SELECT execStart FROM {QUEUE_TABLE} WHERE id = %(masterJobId)s"
                    params = {"masterJobId": pid}
                    rows = self._exec(query, params)
                    if rows["execStart"] is None:
                        # Record master job start
                        self.update_job(pid, {"execStart": updated_values["execStart"]})

            elif updated_values["status"] == "DONE":
                job_time = datetime.utcnow()
                if job_to_update["status"].upper() == "MASTER" or "testing" in job_to_update["kwargs"]:
                    # Do not update the LatestReportExecution table for these jobs. All other jobs are updated with the
                    # execEnd time by the main executor in the `jobs_missing_latest_execution_update` function.
                    updated_values["execEnd"] = job_time

            elif updated_values["status"] == "CANCELED":
                # If a job is canceled all other jobs in that dependency graph, currently
                # not in the DONE state should get the same status

                # Get masterJobId or simply the job id if this is a master job
                pid = job_to_update["masterJobId"] or job_to_update["id"]
                if pid is not None:
                    # A dependent job failed
                    msg = updated_values.get("message", None)  # Get a user-defined message if exists
                    self.update_graph_with_status(pid, job_id, updated_values["status"], msg)

            elif updated_values["status"] == "FAILED":
                # If a job fails all nodes that are directly dependent upon that job should
                # also fail with a message explaining that a dependent job has failed.
                #
                #           A
                #         /   \
                #       B1     B2
                #      /  \     \
                #    C1    C2    D1
                #
                # In the graph above if job C1 fails then B1 and A fails. C2 does not fail
                # since we can reuse that execution if we resubmit the job. The branch B2-D1
                # does not fail for the same reason.
                job_time = datetime.utcnow()
                updated_values["execEnd"] = job_time
                if job_to_update["masterJobId"] is not None:
                    failed_id = job_to_update["id"]
                    failed_master_id = job_to_update["masterJobId"]
                    # Fail master job - This will run on each recursive run but should not be a big deal
                    self.update_job(failed_master_id, {"status": "FAILED", "message": "A dependent job failed!"})

                    # Find the jobs that depend upon the failed job and mark them as failed
                    query = f"SELECT * FROM {QUEUE_TABLE} WHERE masterJobId = %(masterJobId)s AND JSON_CONTAINS(dependencies, JSON_ARRAY(%(failed_id)s))"
                    params = {"failed_id": failed_id, "masterJobId": failed_master_id}
                    dependent_jobs = self._exec(query, params)

                    # If only a single row is returned make it a list of one
                    # element so the iteration below works
                    if isinstance(dependent_jobs, dict):
                        dependent_jobs = [dependent_jobs]

                    for dep_job in list(dependent_jobs):
                        # The LIKE operator will match the expression %5% with 15, 25 and any other number containing a 5.
                        # We need to ensure the failed job ID is really a dependency of this job
                        if failed_id in dep_job["dependencies"]:
                            # Recursive call up the branch to fail jobs
                            payload = {"status": "FAILED"}
                            try:
                                payload["message"] = updated_values["message"]
                            except KeyError:
                                payload["message"] = "This job failed since a dependent job failed"
                            self.update_job(dep_job["id"], payload)

        # Automatically generate SQL query based on current values in updated_values
        query = "UPDATE StauQueue SET {} WHERE id = %(job_id)s".format(
            ", ".join("{key}=%({key})s".format(key=k) for k in updated_values)
        )
        self._exec(query, {"job_id": job_id, **updated_values})
        LOG.debug(f"Finished processing update request for job {job_id} in {time.time() - processing_start}s")

        # Get updated query
        return self._exec(f"SELECT * FROM {QUEUE_TABLE} WHERE id = %(job_id)s", {"job_id": job_id})

    def change_job_status(self, job_id, new_status):
        """Change the status of a job

        Convenience function to only update the status of a job.

        Args:
            job_id (int): Job identifier
            new_status (str): The new job status after the update

        Returns:
            job (dict): Job information after the update
        """
        return self.update_job(job_id, {"status": new_status})

    def rerun_job(self, job_id):
        """Rerun a job that has failed

        This function takes a job that has failed and moves it to the queued
        status while updating the relevant parameters.

        Args:
            job_id (int): Job identifier

        Returns:
            job (dict): Job information after the update
        """
        return self.update_job(job_id, {"status": "QUEUED"}, rerun=True)

    def update_graph_with_status(self, job_id, failed_job_id, status, msg=None):
        """Update the entire graph to a new status

        The dependency graph for a single job can become quite complicated.
        This function moves the status of the job and all dependencies in a recursive
        way to the supplied status.

        Args:
            job_id (int): ID of the job being updated
            failed_job_id (int): ID of the job that failed
            status (str): The status to give all jobs in graph

        Note:
            No checks are performed on the status. The jobs can move to any status.
        """
        # Check the object does exist
        query = f"SELECT status, dependencies FROM {QUEUE_TABLE} WHERE id = %(job_id)s"
        obj = self._exec(query, {"job_id": job_id})
        if len(obj) == 0:
            raise KeyError(f"The job {job_id} does not exist")

        # Break if job has already completed with success
        if obj["status"] == "DONE":
            return None

        dep = obj["dependencies"]
        if dep is not None and len(dep) > 0:
            for d in dep:
                # Update all dependencies recursively
                self.update_graph_with_status(d, failed_job_id, status)

        # Update the job itself - There is not reason to acquire a lock in this case as the dependent jobs will never
        # change status since their dependencies have also failed. So the gentleman's agreement between executors is
        # ignored in this case.
        if msg is None:
            msg = f"Dependent job {failed_job_id} failed or was canceled"
        query = (
            f"UPDATE {QUEUE_TABLE} SET status=%(status)s,execEnd=%(execEnd)s,message=%(message)s WHERE id = %(job_id)s"
        )
        params = {"status": status, "execEnd": datetime.utcnow(), "message": msg, "job_id": job_id}
        self._exec(query, params)

    def check_dependencies(self, job_id):
        """Check if a job is ready to execute

        Jobs can execute if all dependencies are resolved/known and the job
        is not locked by another process.

        Args:
            job_id (int): The job id to check for resolved dependencies

        Returns:
            out (bool): True if all dependencies are resolved otherwise False

        Raises:
            KeyError: If the job id does not exist
        """
        obj = self.get_job(job_id)
        if len(obj) < 1:
            raise KeyError(f"The job {job_id} does not exist")

        ready_to_execute = False
        if obj["dependencies"] is None:
            # A job that does not know its dependencies is not ready to run
            return ready_to_execute
        elif len(obj["dependencies"]) == 0:
            # No dependencies - Job is ready
            ready_to_execute = True
        else:
            # Count the number of non-completed jobs that this job depends upon. The job dependencies are listed
            # as a JSON list in the dependencies column. Here all ids that are not done in that list are counted.
            query = """
            SELECT count(id) as job_count
            FROM StauQueue
            WHERE JSON_CONTAINS(
              (
                 SELECT dependencies->'$[*]'
                 FROM StauQueue
                 WHERE id = %(job_id)s
              ),
              JSON_ARRAY(id)
              )
            -- A job moves to DONE once the work has finished but the dependency isn't resolved until the orchestrator
            -- has updated the LatestReportExecution table for the work IDs involved. Once that happens execEnd is set.
            AND execEnd IS NULL
            """
            params = {"job_id": job_id}
            missing_jobs = self._exec(query, params)
            # If all dependencies are in DONE state this job is ready to run
            if missing_jobs["job_count"] == 0:
                ready_to_execute = True

        return ready_to_execute

    def jobs_missing_latest_execution_update(self):
        """Get list DONE jobs without an execEnd

        The jobs have finished executing but work_ids in the LatestReportExecution table are not
        updated. This is a serial task that is handled by the scheduler function.
        """
        query = f"""
        SELECT id, kwargs, jobType, updatedAt
        FROM {QUEUE_TABLE}
        WHERE status ='DONE' and execEnd is NULL
        """

        jobs = self._exec(query, {})
        if jobs is None:
            # Nothing to update
            LOG.info("No jobs are missing latest execution information")
            return

        if isinstance(jobs, dict):
            # If a single job is returned ensure the iteration still works
            jobs = [jobs]

        for itt, job in enumerate(jobs):
            LOG.debug(f"Updating latest execution for dependencies of id {job['id']}")
            # Transform range of work ids to full list
            try:
                work_ids = range_to_work_ids(job["kwargs"]["work_ids"])
            except TypeError:
                msg = f"Cannot convert work_ids of job '{job['jobType']}' (id: {job['id']}) to list, was {type(job['kwargs']['work_ids'])}."
                raise TypeError(msg)

            # Update the dependencies with the updatedAt timestamp as that is likely when the job finished
            self.update_latest_execution_information(
                work_ids, job["jobType"], job["updatedAt"].strftime("%Y-%m-%d %H:%M:%S")
            )

            # Update the main job
            query = f"UPDATE {QUEUE_TABLE} SET execEnd = %(execEnd)s WHERE id = %(jobId)s"
            params = {"execEnd": job["updatedAt"], "jobId": job["id"]}
            self._exec(query, params)

        LOG.info("Finished updating latest execution information")

    def update_latest_execution_information(self, work_ids, job_type, latest_exec=None):
        """Update the latest execution information

        Update the carExecStatus table with information about which report/job
        has executed for a given set of work ids. For each id in work ids this
        function will update the report type with the current time as the latest
        execution time. If the report type does not exist it is created.

        Args:
            work_ids (List):   List of unique work ids
            job_type (str): Name of the executed job
            latest_exec (str): The latest execution timestamp as a string. Default: UTC now
        """
        if len(work_ids) == 0:
            raise RuntimeError("Cannot update execution time. List of work ids is empty")

        current_timestamp = latest_exec or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # This update is done in a loop (and not using the execmany command) as the ON DUPLICATED KEY UPDATE
        # syntax does not work in the execmany command
        value_list = []
        for wid in work_ids:
            # Returns a list with the MySQL command as string:
            # [
            #   (work_id_1, '{ "report_1": "2020-01-01 00:00:00" }'),
            #   ...
            # ]
            value_list.append("({}, '{}')".format(wid, json.dumps({job_type: current_timestamp})))

        car_values = ",".join(value_list)
        # Update multiple rows in a single query. `car_values` is expanded to a series of tuples VALUES (t1), (t2), (t3)...
        # Each is inserted and in case of unique key constraint the value is updated.
        # See: https://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html
        base_statement = f"""
            INSERT INTO {EXECUTE_TABLE} (workId, history) VALUES {car_values}
            ON DUPLICATE KEY UPDATE history = JSON_SET(history, '$.{job_type}', '{current_timestamp}')
            """
        self._exec(base_statement, {})

    def __enter__(self):
        """Return the StauQueue object

        This function is designed to call within a with statement,

        with StauQueue() as queue:
            queue.add_job(my_job)
        """
        return self

    def __exit__(self, type, value, traceback):
        """Clean up before leaving a with statement

        Any exceptions raised within the with statement are not handled by
        the __exit__ method and must be processed by the caller.
        """
        self.db_conn.close()

    def close(self):
        """Close active database connection"""
        self.db_conn.close()


def range_to_work_ids(range_ids):
    """Convert list of work id ranges to full list

    This function takes a list of ranges and converts it to the full list
    of work ids. Returns the original list if conversion is not possible.

    Both ends of each range is included in the final list.

    Args:
        range_ids (list): List of work id ranges as tuple

    Returns:
        work_ids (list): Full list of all work ids

    Examples:
        > range_to_work_ids([(0, 4), (7, 9), (11,)])
          [0, 1, 2, 3, 4, 7, 8, 9, 11]
        > range_to_work_ids([0, 1, 2, 3, 4, 7, 8, 9, 11])
          [0, 1, 2, 3, 4, 7, 8, 9, 11]
    """
    all_ids = []
    try:
        for cur_range in range_ids:
            try:
                # The current range is a full tuple with a start and end
                start, end = cur_range
                all_ids += list(range(start, end + 1))
            except ValueError:
                # The current range is only a single object. E.g. (22,). This will only insert 22 to the list
                all_ids += cur_range

    except TypeError:
        if isinstance(range_ids, list):
            # Return original list
            all_ids = range_ids
        else:
            raise
    return all_ids


if __name__ == "__main__":
    queue = StauQueue()
    queue.jobs_missing_latest_execution_update()
