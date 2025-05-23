# Metadata Service Provider

This page summarizes how the `ServiceMetadataProvider` integrates with Metaflow.

## Registration
The provider is registered under the name `service` so it becomes the default provider when the metadata service is enabled:

```python
# metaflow/plugins/__init__.py
```
```python
# metaflow/plugins/__init__.py
METADATA_PROVIDERS_DESC = [
    ("service", ".metadata_providers.service.ServiceMetadataProvider"),
    ("local", ".metadata_providers.local.LocalMetadataProvider"),
]
```

## Runtime integration
When a flow is executed, the runtime obtains a run id from the provider and registers it if necessary:

```python
# metaflow/runtime.py
    if run_id is None:
        self._run_id = metadata.new_run_id()
    else:
        self._run_id = run_id
        metadata.register_run_id(run_id)
```

The runtime then starts a heartbeat that keeps the run alive:

```python
# metaflow/runtime.py
@contextmanager
def run_heartbeat(self):
    self._metadata.start_run_heartbeat(self._flow.name, self._run_id)
    yield
    self._metadata.stop_heartbeat()
```

Each task also starts its own heartbeat and registers metadata about the attempt:

```python
# metaflow/task.py
self.metadata.start_task_heartbeat(self.flow.name, run_id, step_name, task_id)
...
self.metadata.register_metadata(
    run_id,
    step_name,
    task_id,
    metadata,
)
...
# terminate side cars
self.metadata.stop_heartbeat()
```

Artifacts produced by a task are registered with the provider via `TaskDataStore`:

```python
# metaflow/datastore/task_datastore.py
self._metadata.register_data_artifacts(
    self.run_id, self.step_name, self.task_id, self._attempt, artifacts
)
```

### Task cloning
Utilities that clone tasks also register task IDs and metadata about the origin
task:

```python
# metaflow/clone_util.py
metadata_tags = ["attempt_id:{0}".format(attempt_id)]
output.clone(origin)
_ = metadata_service.register_task_id(
    run_id,
    step_name,
    task_id,
    attempt_id,
)
metadata_service.register_metadata(
    run_id,
    step_name,
    task_id,
    [
        MetaDatum(
            field="origin-task-id",
            value=str(origin_task_id),
            type="origin-task-id",
            tags=metadata_tags,
        ),
        MetaDatum(
            field="origin-run-id",
            value=str(origin_run_id),
            type="origin-run-id",
            tags=metadata_tags,
        ),
        MetaDatum(
            field="attempt_ok",
            value="True",  # Cloned tasks are always marked successful
            type="internal_attempt_status",
            tags=metadata_tags,
        ),
    ],
)
```

## Client features
Client-side APIs call helper methods implemented by the provider. For instance, runs can filter tasks by metadata and mutate tags atomically:

```python
# metaflow/client/core.py
for step in steps:
    task_pathspecs = self._metaflow.metadata.filter_tasks_by_metadata(
        flow_id, run_id, step.id, metadata_key, metadata_pattern
    )
...
final_user_tags = self._metaflow.metadata.mutate_user_tags_for_run(
    flow_id, self.id, tags_to_remove=tags_to_remove, tags_to_add=tags_to_add
)
```

The provider sends heartbeat requests, registers artifacts and metadata, and performs tag mutations:

```python
# metaflow/plugins/metadata_providers/service.py
self.sidecar.start()
self.sidecar.send(Message(MessageTypes.BEST_EFFORT, payload))
...
def register_data_artifacts(self, run_id, step_name, task_id, attempt_id, artifacts):
    url = ServiceMetadataProvider._obj_path(self._flow_name, run_id, step_name, task_id)
    url += "/artifact"
    data = self._artifacts_to_json(run_id, step_name, task_id, attempt_id, artifacts)
    self._request(self._monitor, url, "POST", data)

def register_metadata(self, run_id, step_name, task_id, metadata):
    url = ServiceMetadataProvider._obj_path(self._flow_name, run_id, step_name, task_id)
    url += "/metadata"
    data = self._metadata_to_json(run_id, step_name, task_id, metadata)
    self._request(self._monitor, url, "POST", data)

def _mutate_user_tags_for_run(cls, flow_id, run_id, tags_to_add=None, tags_to_remove=None):
    url = ServiceMetadataProvider._obj_path(flow_id, run_id) + "/tag/mutate"
    # retry logic omitted
    resp, _ = cls._request(None, url, "PATCH", data=tag_mutation_data, return_raw_resp=True)
```

The `ServiceMetadataProvider` implements all HTTP communication with the metadata service and handles background heartbeats using a sidecar thread. Runtime and client modules call its methods to create runs, register tasks, record metadata and artifacts, and query or modify data stored in the service.
