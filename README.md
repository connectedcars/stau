
![StauLogo.png](https://github.com/connectedcars/stau/blob/master/assets/StauLogo.png?raw=true)

**The last mile to production is often the hardest - but it doesn't have to be.**

Checkout the [video](https://player.vimeo.com/video/667304698?h=c7f68a85b3) or [slides](https://junckerbraedstrup.dk/pydata-global-Stau-large.html) from the PyData Global 2021 talk on Stau.

# Introduction
Stau, german for traffic congestion, is a lightweight tool for orchestrating real-world data science jobs. It acts as a barebone orchestration system supporting job scheduling, advanced dependency management, parallel execution of work, and more. The core code is only ~1500 lines of Python and built to quickly onboard new team members.

The design bundles all components into a single executor (`src/stau_executor.py`) which can schedule jobs, resolve dependencies, execute work, and run in multiple copies. The tool is currently running production workloads at [Connected Cars](https://www.connectedcars.io), processing 4000+ jobs and hundreds of millions of measurements per day. The design supports most mid-size data science workloads and attempts to remove as much complexity as possible.

**Key features,**

 - Build with simplicity in mind
 - Pipelines are generated from source code - No pipeline files required
 - Keyword arguments are stored on all job executions allowing jobs to run with the same arguments at any time

Stau supports any type of job - ETL, model evaluation, sending emails, you name it. At its core Stau simply executes Python files. These files can contain any logic imaginable. If you want to execute non-Python jobs simpy write a Python wrapper to execute and error handle the underlying process (e.g. shell script, Big Query job, ...). By chaining multiple files together through dependencies it's possible to build complex workflows that automatically follow code changes.

## About this release
This repository contains a one-time release of Stau. It showcases how to design dynamic pipelines from source code and architect the distributed system. To keep the complexity and code footprint low Stau ships without some central components: Monitoring and DevOps pipelines. Since each company/team uses different tools and environments it didn't make sense to build these directly into Stau.

Instead, Stau is designed to integrate into your existing code base and morph into the tool you need. That makes it quick to add your custom logic directly into Stau without having to write custom plugins.

Read the sections **Monitoring jobs** and **Running Stau in production** for tips on handle these subjects.

## Compared with other frameworks
There are plenty of open-source data orchestration tools available - so why Stau?
For a team that already has a substantial codebase migrating to AirFlow or Prefect is a large task. These tools are great but can come with quite a bit of DevOps and are best for large-scale setups or greenfield deployments. Stau is built for small to medium-scale where the complexity of your infrastructure shouldn't exceed the complexity of your code.

Due to the complexity of such tools, many teams set up dedicated DevOps or deployment engineers. In a small team that isn't an option. Therefore, Stau assumes that everyone in the team deploys and would rather spend their time doing something else. In a perfect world, developers only have to focus on their code and leave the pipelining work up to Stau.

## Basic principals in Stau

 - All Python scripts must live in a subfolder named **src**. It's possible to change this, if required.
 - All job types must match a Python script filename
 - All main functions must accept the ****kwargs** argument
 - The database is the integration layer - Jobs cannot directly parse information between each other
 - All jobs must define a variable called `STAU_CONFIG`

## Terms in Stau

 - **Master job**: The initial job that was added to the queue. The master job is the main work that Stau expands to the full execution graph. After the graph is build the job is kept as a placeholder for the graph but never executes ifself.
 - **work ID**: Unique identifiers that Stau uses to split work into smaller chunks. work IDs could be customer IDs, purchase IDs, or similar depending on the setting.
 - **Main function**: A Python function that starts a specific job. Similar to the main function concept used in many compiled languages.
 - **Pipeline or Graph**: A directed graph containing one or more jobs. The first node in the graph is the **master job**.
 - **Job or jobType**: The individual Python scripts to execute. A job always maps to a single Python file.
 - **Chunk**: A subtask of a job. Jobs are split based on work IDs into chunks that can execute independently of each other. All chunks in a job are identical except for the work IDs they execute on. A job is always split into one or more chunks.
 - **Orchestrator**: Stau instance responsible for adding jobs, maintaining the queue, and more.
 - **Worker**: Stau instance responsible for executing the actual work.

## High-level design
Stau is designed to generate execution pipelines on the fly directly from source code. By isolating individual tasks into separate Python files and embedding metadata alongside the code Stau runs without any pipeline files.

This design has multiple advantages,

- Pipeline files never get outdated nor need maintenance
- Pipelines are version controlled and always follow changes to the code
- Pipelines can start from any point since they are not fixed

Automatically generating pipelines may lead to circular dependencies where multiple jobs depend upon each other before they can execute (`A->B->C->A`). Stau prevents such situations by checking for circular dependencies when constructing the graph. If an error is detected Stau alerts about the issue.

The Stau executor is able to run in two distinct modes: **orchestrator** and **worker**. Each mode has distinct responsibilities. The orchestrator is responsible for,

- Moving jobs between states (E.g. from `QUEUED` to `PENDING`)
- Checking for jobs that failed
- Adding scheduled jobs
- Resolving job dependencies

The workers are simply responsible for performing the actual work required to complete a chunk.

Since all executors in Stau are identical the responsibilities are decided by a simple capture the flag schema. All executors attempt to acquire the same database lock. The first executor to get the lock becomes the orchestrator. If the orchestrator terminates for any reason the lock is released and the remaining processes will fight for the lock until a new orchestrator is found.

## Designing idempotent pipelines
In Stau pipelines should run without side effects. Such pipelines are often called [idempotent](https://en.wikipedia.org/wiki/Idempotence) or impotent. Jobs that run multiple times in a row should leave the storage system in a desirable state. E.g. if the same job executes with the same parameters multiple times it should leave the database in the same state as if it had only executed once. Otherwise, the final database state is dependent on the number of identical executions which leads to problems with further processing the data.

There are two common ways to make jobs impotent,

 - **Upsert**: Define unique dimensions in your data. E.g. if you compute an average uptime for each work ID per day then your unique dimensions are work ID and date. Having multiple uptimes for the same work ID per day has no meaning. In this case, overwrite the previous data.
 - **Delete**: If the data doesn't have any unique dimensions the other option is to remove old entries before inserting new.

# Technical documentation
![staudiagram.png](https://github.com/connectedcars/stau/blob/master/assets/StauDiagram.png?raw=true)
## The metadata section

Each job in Stau must define a metadata section called `STAU_CONFIG`. This section is used by Stau to construct the dependency graph and contains important execution information about the job.

This section mainly contains,

- **Job type**: Name of the job. Normally matches the filename without extension.
- **chunk size**: Each execution of the job should contain this number of work IDs.
- **dependencies**: List of other job types that this job depends upon.
- **main function**: The main Python function to call when starting the job.
- **data sources, optional**: Human readable list of locations the job reads from. Not used by Stau for any processing.
- **data sinks, optional**: Human readable list of locations the job writes to. Not used by Stau for any processing.

The dependency section contains some additional parameters,

- **max_lag**: Add this dependency to the graph if it hasn't been executed within the last `max_lag` minutes for all work IDs in the current chunk.
- **custom_kwargs**: Fixed keyword arguments that this dependency and all nodes below it should execute with. If a job below the dependency defines the same argument this value is ignored.

To handle custom dependency resolution the metadata section also supports the arguments: `allow_missing_work_ids`, `unique`, `allow_multiple_jobs`. These are covered in more detail in the chapter **Handling graph edge-cases**.

## Reproducibility
In some situations, it's important to know exactly which code version and arguments a specific job was executed with. To support this Stau stores both the Git commit SHA, work IDs, and all keyword arguments used alongside the job information. It's, therefore, possible to rerun the same job in the future.

## Dependency resolution
**Function**: `stau_executor.resolve_dependencies`

Resolving dependencies is a three-step process. First, the raw graph is constructed that contains all possible jobs that may need execution. It isn't given that all jobs in this graph will actually execute. Then work IDs relevant for the execution are extracted. Finally, all jobs that don't need to execute are removed and the graph is split for parallel execution.

When a new master job is added to Stau the orchestrator will,

 - Read the job type
 - Import the matching Python file with the same name as a module
 - Read the `STAU_CONFIG` variable and extract the `dependencies`
 - Check if any dependencies are currently waiting in the queue. In this case, Stau waits until the dependencies have finished since their final state is important for the resolution step. If the resolver has to wait too long a warning is raised.
 - For each dependency
   - Import the matching Python file
   - Read the `dependencies` from the `STAU_CONFIG` variable
   - Continue recursion until no more dependencies are found
- Combine the dependency graphs from each dependency subgraph

After the first stage, all possible jobs that may need to execute are nodes in the graph.

![staudependencyresolutionexamplefullgraph.svg](https://github.com/connectedcars/stau/blob/master/assets/StauDependencyResolutionExampleFullGraph.svg?raw=true)

The next step involves determining the required work. In Stau each dependency graph executes on a fixed set of work IDs. These IDs are simple unique whole number references that each job understands. Examples of work IDs are customer IDs, unit IDs, transaction IDs, or product IDs. The only rule is that IDs must be whole numbers and the same ID must be used in all jobs. Stau doesn't support multiple ID types. The IDs often map 1:1 with primary keys in a database table.
Each job can define a custom function (`select_work`) that returns the IDs relevant for that job. The function should exist on the master job, i.e. the job that was originally added. Otherwise, it isn't used. If the master job doesn't define the `select_work` function IDs are retrieved from a generic function (`stau_executor.get_default_work_ids`).
Once the work IDs are found they apply to the entire graph! All jobs are executed on the same work IDs as defined by the master job.

The final, and most complicated, step is chunking or splitting the graph into parallelizable tasks. First, the work IDs from step two are compared against the largest chunk size for all jobs in the graph. This determines the number of required graphs. If all jobs have the same chunk size the graph is split into independent graphs.
Otherwise, the graphs may share one or more common nodes. The work IDs are then split onto each subgraph if multiple graphs exist.

Each graph is then constructed top-down. The first job/node is selected and all work IDs are split into chunks using the job's own chunk size. If any work ID in the current chunk hasn't executed within the required timeframe (`max_lag`) the job is added to the graph. Otherwise, it's simply ignored. This step is recursive. All jobs below a specific branch are ignored if the parent job doesn't need to execute.

From the previous example, the job C has already executed within the required time range (`max_lag`). Therefore the `A->C->D` branch is pruned since it's up to date. Only the `A->B->E` branch remains,

![staudependencyresolutionexamplefilteredgraph.svg](https://github.com/connectedcars/stau/blob/master/assets/StauDependencyResolutionExampleFilteredGraph.svg?raw=true)

The jobs are now split based on the individual chunk size and the requested work IDs. In this simple example work IDs are `[1,2,3,4,5,6]`. Job A has a chunk size of 3, job B has a chunk size of 2, and job E has a chunk size of 6. Based on this the jobs are split. Since chunk `B1` contains work IDs 3 and 4 it becomes a dependency for both `A0` (`[1,2,3]`) and `A1` (`[4,5,6]`).
Note the additional job A in white. This is the master job that initiated the graph. Once the graph is built it moves to the `MASTER` state and remains as a placeholder for the graph. It cannot execute since it doesn't contain any work IDs but finishes once all chunks finish. If a chunk fails the master job also fails.

![staudependencyresolutionexamplechunkedgraph.svg](https://github.com/connectedcars/stau/blob/master/assets/StauDependencyResolutionExampleChunkedGraph.svg?raw=true)

Once the graph is constructed each job is inserted as a separate row in the database table starting from the highest level node.

The above graph results in the following database rows,

**Table: StauQueue**
| id     | jobType | status  | kwargs                  | dependencies |
|--------|---------|---------|-------------------------|--------------|
| 0      | A       | MASTER  |                         | [1,2]        |
| 1      | A       | QUEUED  | `{"work_ids": [1,2,3]}` | [3,4]        |
| 2      | A       | QUEUED  | `{"work_ids": [4,5,6]}` | [4,5]        |
| 3      | B       | QUEUED  | `{"work_ids": [1,2]}`   | [6]          |
| 4      | B       | QUEUED  | `{"work_ids": [3,4]}`   | [6]          |
| 5      | B       | QUEUED  | `{"work_ids": [5,6]}`   | [6]          |
| 6      | E       | QUEUED  | `{"work_ids": [1,..,6]}`|              |

## Job life-cycle in the queue
**Functions**: `stau_executor.groom_queue` and `stau_executor.process_queue`

After the job is resolved and waiting in the queue the orchestrator ensures a proper execution sequence. Initially, all jobs are in the `QUEUED` state. The orchestrator scans the queue and checks dependencies on all `QUEUED` jobs to evaluate if any jobs are ready to execute. A job is ready to execute if all its dependencies have executed with success or it doesn't have any dependencies. The orchestrator moves such jobs to `PENDING`. This indicates to workers that the job is ready for them to run.

The workers scan the queue for any `PENDING` jobs. If found, they first lock the job to ensure it doesn't execute twice. Then they load the Python script matching the job type, moves the job to `RUNNING`, and runs the main function. If the job runs without issue the worker sets the job to `DONE`. Otherwise, the job is marked as `FAILED`.

When a job finishes successfully the worker will update the latest execution time for all work IDs in the job. This information is stored in the `StauLatestExecution` table. If the job fails this step is skipped.

## Handling graph edge-cases
Real-world data science pipelines can get quite complicated when dependencies grow and multiple pipelines are automatically generated simultaneously. To support these edge cases Stau implements a few keywords to guide the scheduler. These arguments are available on the `stau_utils.ReportDependencies` or `stau_utils.ReportConfig` class.

### Using the `allow_missing_work_ids` argument
It happens that dependent jobs need to execute on different work IDs. Let's take the example where job A depends upon job B. Job B executes 30 minutes before job A but only executes on work ID 1 and 2. Then job A is submitted on work ID 1, 2, and 3. 
If Stau executed normally, it would resolve the dependencies for job A and determine that work ID 3 has never executed job B. It will therefore add job B to fix the dependency. However, if work ID 3 doesn't support job B that may result in an inconsistent state.

Using the `allow_missing_work_ids` argument on the dependency tells Stau to simply check that job B executed within the required time but that it should ignore the actual work IDs involved in the execution.

### Using the `unique` argument
Imagine the following situation,

![Basic graph to showcase using the unique argument](https://github.com/connectedcars/stau/blob/master/assets/GraphLayoutExample1.svg?raw=true)

Job D appears twice in the graph along two distinct branches from job A. This graph may resolve to two distinct but equally valid graphs. One where job D is shared and one where job D is unique to the branch.

![Result of using the unique argument](https://github.com/connectedcars/stau/blob/master/assets/GraphLayoutUniqueSetting.svg?raw=true)
  
Using the `unique` keyword on the branch provides a distinction between the two cases. The default is to merge the two jobs and only execute a single shared copy on job D.

### Using the `allow_multiple_jobs` argument
Job A runs every 5 minutes and takes 2 - 7 minutes to execute depending on the general system load. Using `allow_multiple_jobs=True` tells the Stau scheduler to add a new copy of job A while a current version is already running. Otherwise, Stau will simply skip the next execution and attempt to schedule the job again 5 minutes later.

### Waiting to resolve when dependencies are queued
Job B was added to the queue 1 hour ago but hasn't executed yet since the queue was busy. Now job A, that depends upon job B, is added. Since job B hasn't executed yet job A would resubmit job B to complete the dependency. This would lead to multiple copies of job B. That is wasteful and could lead to data issues. In such cases, Stau keeps job A from resolving until job B has executed. Once that happens job A is "unlocked" and the normal dependency resolution logic kicks in.

## Parallelism in Stau
Stau relies on task parallelization to speed up execution. Each job is split into independent chunks based on the work IDs for the entire graph. The execution of the individual chunks is then kept back until all dependencies are met.

This is a great option for small to mid-size data science workflows where data parallelization is too complicated. Depending on the execution environment each job is Stau has full freedom to perform data parallelization internally. However, jobs run with threads from the main Stau process. Adding an additional parallelization layer at that point may lead to unforeseen complications.

Unfortunately, large aggregations cannot task-parallelize. If the runtime of large aggregations becomes a problem, users should parallelize internally. Either by adding multi-processing directly to the job or by moving to a storage system built for such operations (e.g. Hadoop, Spark, or Google Big Query).

## Scheduling recurring jobs
Jobs are scheduled using [APScheduler](https://apscheduler.readthedocs.io/en/3.x/) that supports multiple triggers. All jobs are stored as simple dictionaries in `src/stau_schedule.py`. This makes it easy to review the schedule and ensure it aligns with changes to the code.

**Example of a recurring job**
``` python
{
    "id": "Say goodbye id",
    "name": "Say goodbye every minute",
    "trigger": CronTrigger(minute='*/1'),  # Run every minute
    "kwargs_stau_api": {
        "jobType": "job_goodbye_work",
        "kwargs_func": {
            "final_message": "Thanks for trying Stau!"
        },
    }
}
```

A job has the following fields,

- **id**: Unique identifier for the job. Normally, identical to the name.
- **name**: User-friendly name of job.
- **trigger**: Determines when the job should execute using the APScheduler `CronTrigger` or `IntervalTrigger` syntax.
- **kwargs_stau_api**: Stau specific section
  - **jobType**: Name of the job. Must match a Python filename in `src/`.
  - **kwargs_func** (Optional): Keyword arguments to apply when calling the main function.
  - **dependencies** (Optional): Used to hardcode specific job IDs that this report should depend upon. Normally only used for debugging or very custom one-time executions. If omitted dependencies are automatically resolved.

When the Stau orchestrator starts it will read the schedule and update the matching database table (`StauRecurringJobs`) in case of changes. APScheduler then keeps track of time and adds the jobs when needed.

The schedule has a grace period when adding jobs. If the scheduler misses an execution, perhaps due to a new deployment, the job is automatically added once the orchestrator is back online.

## Monitoring jobs
Since monitoring tools are very company and team-specific Stau ships without build-in monitoring. Instead, most information is captured within the three Stau tables. This makes it quick to integrate Stau into any monitoring stack.

Stau itself uses the Python logging module to emit informative messages about failures or execution states. When running Stau in production it is highly suggested to forward `ERROR` and `WARNING` messages to a real-time messaging service (such as Slack). These levels require action and may result in failed executions. If a job fails a full traceback is also logged as an error-level message. The traceback is also written directly to the job information in the `StauQueue` table.

## Handling failure
When jobs fail they are placed in the `FAILED` state. Failures cascade up through the graph while any work that isn't dependent upon the failure is allowed to complete. Information about the failure is both logged directly from the worker and added alongside the job information in the database.

![dependencydiagram-animated.svg](https://github.com/connectedcars/stau/blob/master/assets/DependencyDiagram-animated.svg?raw=true)

Stau will only retry the failed job if it's part of a future dependency graph. Once the dependency is required to fulfill a new dependency graph Stau will detect the missing dependency and add retry the job. This is convenient for short-lived failures (e.g. database issues).
However, if the failure was due to a larger issue (e.g. poor database state, syntax errors, or a hidden bug) the new graph will also fail. This will keep happening until the underlying issue is solved. This normally isn't a problem unless the failing job executes often or is a central dependency for other jobs.

If a worker dies while executing a job, the job will remain in the `RUNNING` state. Once a job enters the `RUNNING` state it's the worker's responsibility to update the job state. If the worker suddenly terminates without warning this can result in missing execution. To ensure Stau catches such failures each worker regularly checks in with the database to highlight that the job is still running. If the worker hasn't checked in within the expected interval the orchestrator assumes the job has failed and releases it. The job is then available for other workers.

# Running your own Stau instance
The following guide is written for Linux-based systems. Setting up Stau on Windows or Mac OS X may differ.

## Setting up MySQL
First, ensure you have a running MySQL server. This can either be a local or remote instance.

If you don't have a testing database available set up a containerized database using Docker is likely the fastest route. First, install Docker and docker-compose.

From the reposotory root folder run,

```
docker-compose up -d db
```

To start up the database. Wait around ~30 seconds for the database to start. Then create the required database tables by running,

```
docker-compose up migrate
```

You now have all tables required to run Stau. You can attach to the database by using `localhost` as the host and the testing credentials listed in the `.env` file. You can either use the `mysql-client` command-line tool or the GUI of your choice.

## Adding required environment variables
The Stau demo requires several environment variables. These are listed in the `.env` file. First make sure they match your database setting used above. Then simply export each variable to your local system,

```
export MYSQL_DATABASE=test
... all all variables from .env
```

## Setting up Python
Stau requires Python >= 3.6 since it uses the data classes. Set up a virtual environment and install the required 3rd party packages.

```
python -m venv stau_environment
source stau_environment/bin/activate
pip install -r requirements.txt
```

Your Python environment is now located in the `env` folder.

## Starting the executors
Finally, run,

```
python src/stau_executor.py
```

If this doesn't work ensure you have sourced the virtual environment using the above `source` command.

This will start the Stau orchestrator and populate the scheduler. Check the `StauRecurringJobs` table to ensure the recurring jobs were added. You should see the two jobs `job_hello_work` and `job_goodbye_work`. This table should always match the `stau_schedule.py` list.

The demo adds the `job_goodbye_work` every minute and the `job_hello_work` every 20 minutes. After a few minutes open the `StauQueue` table. You should see several rows all in the `QUEUED` state. The jobs are not executing since no worker is running. Only the orchestrator is currently running. To start a worker simply create a new shell and start a new instance of the executor,

```
source stau_environment/bin/activate
python src/stau_executor.py
```

You should now see output similar to,

```
> python src/stau_executor.py
Hello work ID 0
Hello work ID 1
Goodbye work ID 0
Goodbye work ID 1
Thanks for trying Stau!
```

Note, since `job_hello_work` has a chunk size of `1` two jobs are created. While only a single job is created for `job_goodbye_work` since its chunk size is `2`. Therefore, the order of the first two lines isn't fixed and work ID 1 can execute before work ID 0.

The demo jobs produce the following graph,

```bash
# Test graph

           job_goodbye_work           # Graph job - never executes
                   |
           job_goodbye_work           # Single job
             /          \
            /            \
job_hello_work       job_hello_work   # Two jobs due to chunk size
```

Once the jobs have executes you should also see two rows in the `StauLatestExecution` table. It contains when each job was executed against different work IDs.

## Running Stau in production
If you plan to run Stau in a production setting it's important to define the `stau_executor.get_default_work_ids` function. This is used to fill in work IDs when the function doesn't define a `select_work` function. Since work IDs are unique to different settings it's impossible to specify which default work IDs are needed.

Due to the way Stau selects the orchestrator it's best to use a [recreate](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment) deployment strategy in Kubernetes compared with a `RollingUpdate`. This ensures all previous Stau executors have terminated before new pods are started.

# Conclusion
The design patterns used in Stau come with the advantage of simplicity and a low entry bar. Once integrated into an existing codebase Stau allows new developers to quickly schedule jobs and extend complicated pipelines without the complexity associated with other job orchestration frameworks.

We hope this repository will inspire others in their quest to optimize their data pipelines!
