# Understanding Resume in Metaflow

Metaflow provides a `resume` command that allows you to continue a previous run by
cloning finished tasks and only re-executing the missing pieces. This document
follows the implementation starting from the command line entry point down to
the runtime logic that performs cloning.

## CLI entry point

The `resume` subcommand is implemented in
[`cli_components/run_cmds.py`](../metaflow/cli_components/run_cmds.py). The
function defines command line options and constructs a `NativeRuntime` object
configured for cloning:

```python
# simplified excerpt
@click.command(help="Resume execution of a previous run of this flow.")
@tracing.cli("cli/resume")
@common_run_options
@click.pass_obj
def resume(
    obj,
    tags=None,
    step_to_rerun=None,
    origin_run_id=None,
    run_id=None,
    clone_only=False,
    reentrant=False,
    max_workers=None,
    max_num_splits=None,
    max_log_size=None,
    decospecs=None,
    run_id_file=None,
    resume_identifier=None,
    runner_attribute_file=None,
):
    before_run(obj, tags, decospecs)
    if origin_run_id is None:
        origin_run_id = get_latest_run_id(obj.echo, obj.flow.name)
        if origin_run_id is None:
            raise CommandException("A previous run id was not found. Specify --origin-run-id.")
    # ... validate step_to_rerun and run_id ...
    runtime = NativeRuntime(
        obj.flow,
        obj.graph,
        obj.flow_datastore,
        obj.metadata,
        obj.environment,
        obj.package,
        obj.logger,
        obj.entrypoint,
        obj.event_logger,
        obj.monitor,
        run_id=run_id,
        clone_run_id=origin_run_id,
        clone_only=clone_only,
        reentrant=reentrant,
        steps_to_rerun=steps_to_rerun,
        max_workers=max_workers,
        max_num_splits=max_num_splits,
        max_log_size=max_log_size * 1024 * 1024,
        resume_identifier=resume_identifier,
    )
    # runtime.clone_original_run() followed by runtime.execute()
```

The command locates the latest run (unless `--origin-run-id` is specified),
creates a `NativeRuntime` in resume mode by supplying `clone_run_id`, and then
invokes `clone_original_run` and `execute` to perform cloning and further
execution.

## Runtime initialization

Inside [`runtime.py`](../metaflow/runtime.py), `NativeRuntime` receives
`clone_run_id` and prepares the data structures used during resume. The comments
explain the overall cloning strategy:

```python
# excerpt from NativeRuntime.__init__
if clone_run_id:
    # resume logic
    # 0. Clone all successful tasks from `clone_run_id` and run the
    #    unsuccessful or missing ones normally.
    # 1. For every task in the current run find the equivalent task in the
    #    origin run and verify if it can be cloned.
    # 2. If cloning is possible, copy metadata from the origin task so the
    #    resumed run looks like a normal run.
    # 3. Tasks that cannot be cloned are executed normally.
    # To speed up cloning, TaskDataStoreSet prefetches the entire origin DAG
    # and selected data artifacts.
    logger("Gathering required information to resume run (this may take a bit of time)...")
    self._origin_ds_set = TaskDataStoreSet(
        flow_datastore,
        clone_run_id,
        prefetch_data_artifacts=PREFETCH_DATA_ARTIFACTS,
    )
```

The runtime keeps a mapping of cloned tasks and a queue of tasks to execute. All
finished tasks from the original run are inspected and, when possible, cloned
before scheduling new execution.

## Cloning tasks

The method `clone_original_run` handles copying of task metadata and artifacts.
It iterates over the dataset of the origin run and creates new tasks for the
current run where applicable:

```python
# excerpt from clone_original_run
self._logger("Cloning %s/%s" % (self._flow.name, self._clone_run_id), system_msg=True)
for task_ds in self._origin_ds_set:
    _, step_name, task_id = task_ds.pathspec.split("/")
    if task_ds["_task_ok"] and step_name != "_parameters" and (step_name not in self._steps_to_rerun):
        # determine if this task is part of an unbounded foreach, etc.
        # prepare cloning inputs
        ...
with futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
    all_tasks = [executor.submit(self.clone_task, step_name, task_id, ...)
                 for ... in inputs]
    futures.wait(all_tasks)
self._logger("%s/%s cloned!" % (self._flow.name, self._clone_run_id), system_msg=True)
self._params_task.mark_resume_done()
```

After cloning is complete the runtime proceeds with `execute()` to run any steps
that were not cloned.

## Resume leader and synchronization

During resume the `_parameters` task coordinates cloning. The first task that
performs cloning becomes the **resume leader**. Other tasks wait until the
leader marks cloning complete. The relevant helper methods are defined in
`Task`:

```python
# simplified excerpt from Task
def is_resume_leader(self):
    assert self.step == "_parameters"
    return self._is_resume_leader

def resume_done(self):
    assert self.step == "_parameters"
    return self._resume_done

def mark_resume_done(self):
    assert self.step == "_parameters"
    assert self.is_resume_leader()
    self._ds._dangerous_save_metadata_post_done({"_resume_done": True}, add_attempt=False)
```

These helpers allow the runtime to coordinate cloning across distributed workers
and ensure all tasks see a consistent view of the resume state.

## Detecting resume in user code

From user code you can check whether the current run was resumed by inspecting
`current.origin_run_id` which is exposed in
[`metaflow_current.py`](../metaflow/metaflow_current.py):

```python
@property
def origin_run_id(self) -> Optional[str]:
    """The run ID of the original run this run was resumed from."""
    return self._origin_run_id
```

If `origin_run_id` is `None` the run was started normally; otherwise its value
indicates the run that was used as the resume origin.

Resume thus combines cloning of previous results with normal execution of the
remaining steps, allowing workflows to continue from a failed or partially
executed run with minimal overhead.
