#!/usr/bin/env python

import os
import typing
from dataclasses import dataclass, field
import importlib
from concurrent.futures import ProcessPoolExecutor

@dataclass
class ReportDependencies:
    """Class that defines how dependencies are described

    Args:
        job_type (str): The name of the report/job script that is a dependency
        max_lag (int): Maximum number of minutes that can pass before the dependency needs to recompute
        allow_missing_work_ids (bool): When resolving dependencies simply require that the report has executed within
                                       `max_lag` but don't resolve which `work_ids` it exectued on (See ch55804)
        custom_kwargs (dict): Custom key-value pairs that apply only to this dependency and it's dependencies
        unique (bool): True if the dependency is unique to it's parent. E.g. clean_data_reports is only relevant
                       for it's direct parrent. Any other job in the dependency graph should resolve it's own copy
                       of clean_data_reports. Otherwise, multiple parents can share the same parent.

    Also see:
      - stau_executor.replicate_branch(): Function that uses the lag information
    """

    job_type: str
    max_lag: int
    allow_missing_work_ids: bool = False  # If changed also see stau_executor.py:L797
    custom_kwargs: typing.Dict[str, str] = field(default_factory=dict)
    unique: bool = False


@dataclass
class ReportConfig:
    """Class that defines configuration parameters for reports

    Args:
        job_type (str): The name of the report/job script which is also the job name/type
        chunk_size (int): How many work items can the job be distributed into
        dependencies (list): Instances of the ReportDependencies class that this report depends upon
        main_func (str): Name of the primary function to start the report
        data_sources (list): List of tablenames the report reads from
        data_sinks (list): List of tablenames the report writes to
        is_streaming (bool): Is the job type a steaming job that should always run
        allow_multiple_jobs (bool): Allow the queue to contain multiple jobs of the same type.
    """

    job_type: str
    chunk_size: int
    dependencies: typing.List[ReportDependencies]
    main_func: str
    data_sources: typing.List[str] = field(default_factory=list)
    data_sinks: typing.List[str] = field(default_factory=list)
    is_streaming: bool = False
    allow_multiple_jobs: bool = True

    def __post_init__(self):
        """Check the class contains valid parameters"""
        if (
            not os.path.exists(f"src/{self.job_type}.py")
            and not os.path.exists(f"{self.job_type}.py")
            and not os.path.exists(f"../src/{self.job_type}.py")
            and not os.path.exists(f"services/src/{self.job_type}.py")
        ):
            raise ValueError(f"The job_type must match a script in the src folder. Value was {self.job_type}")


class ModuleLoader:
    def __init__(self, job_type, isolate_execution=True):
        """Create a module loader instance

        This class implements a memory isolated scope to execute functions within.

        All commands are executed within an isolated process. This is required when dynamically importing modules in
        long running applications. When Python imports a module it creates a reference to it. E.g.

        pd = importlib.import_module("pandas")

        Normally, the garbage collector (GC) cleans memory once the variable (pd) isn't used anymore (e.g. using `del pd`).
        The GC decides if a variable is ready to remove using reference counting. Every time a variable is referenced by
        another it's count increases by one. If the variable isn't referenced anywhere (refcount = 0) it's safe to remove.

        Since modules are complex memory structures they are almost always referenced elsewhere. For this reason, their
        refcount remains above zero even after deleting the direct reference with `del`. In short-lived applications
        this isn't a problem. The memory is cleared once the application terminates. In a long-lived application the
        memory will remain in the background until all memory is used.

        Therefore, this class performs all operations on a separate core using the ProcessPoolExecutor. Memory isolated
        to a specific core is in an ephemeral memory scope. Once the core returns the memory is cleared.

        This ensures the memory doesn't leak while the process is running.

        Args:
            job_type (str): Name of the module to load (e.g. report_positions)
            isolate_execution (bool): Execute functions in a memory isolated scope
        """
        self.job_type = job_type
        self.isolate_execution = isolate_execution

    @staticmethod
    def _load_config(job_type):
        """Load the modules Stau config section

        See `load_report_config` for more.
        """
        try:
            exec_report = importlib.import_module(job_type)
            return exec_report.STAU_CONFIG
        except ImportError:
            raise ImportError(f"The job type {job_type} does not exist.")
        except AttributeError:
            raise AttributeError(
                f"The report {job_type} does not follow the config standard"
            )

    @staticmethod
    def _run_select_work(job_type, **kwargs):
        """Execute the modules select_work function

        See `call_report_select_work` for more.
        """
        report_module = importlib.import_module(job_type)
        return report_module.select_work(**kwargs)

    def _exec(self, func, **kwargs):
        """Execute command against module

        See the __init__ method to understand the isolated execution setting.

        Args:
            func (callable): Function to execute
            kwargs (dict): Keyword arguments parsed directly to func

        Returns:
            out: Direct results from calling the function
        """
        if self.isolate_execution:
            with ProcessPoolExecutor(max_workers=1) as pool_executor:
                active_job = pool_executor.submit(func, self.job_type, **kwargs)

                timeout = 120
                if (
                    active_exception := active_job.exception(timeout=timeout)
                ) is not None:
                    raise active_exception

                return active_job.result(timeout=timeout)
        else:
            return func(self.job_type, **kwargs)

    def load_report_config(self):
        """Load the modules Stau config section

        Any Pyhon file located in the same folder as this function is loadable by suppling the filename of the job.
        The same applies for all modules in the current PYTHON_PATH. For the file to work with all the queue processing
        steps it must contain a variable named `configs` that implements the ReportConfig class.

        Returns:
            config (ReportConfig): Config variable directly from module.

        Raises:
            ImportError: If the module does not exist
            AttributeError: If the modules does not contain a configs class
        """
        return self._exec(self._load_config)

    def call_report_select_work(self, **kwargs):
        """Execute the modules select_work function

        Args:
            kwargs (dict): Optional arguments parsed directly to the select_work function.

        return:
            work (list[int]): List of work IDs to execute against.
        """
        return self._exec(self._run_select_work, **kwargs)
