# Module disk-sweeper 

A Viam module for sweeping disk space across device fleets. 

## Model viam-soleng:disk-sweeper:worker

Provide a description of the model and any relevant information.

### Configuration
The following attribute template can be used to configure this model:

```json
{
"target_path": <string>,
"days_old": <integer>,
"dry_run": <boolean>
}
```

#### Attributes

The following attributes are available for this model:

| Name          | Type   | Inclusion | Description                |
|---------------|--------|-----------|----------------------------|
| `target_path` | string  | Required  | Directory path to scan |
| `days_old` | integer | Optional  | Only delete directories older than this many days |
| `dry_run` | boolean | Optional  | Only log without deleting |

#### Example Configuration

```json
{
  "target_path": "/root/.viam/video-storage",
  "days_old": 30,
  "dry_run": true
}
```

### DoCommand

The worker supports the following commands via the `do_command` method:

#### status
Get current configuration and statistics:

```json
{
  "command": "status"
}
```

#### analyze
Analyze directories and identify orphans:

```json
{
  "command": "analyze"
}
```

#### sweep
Execute sweep (respects `dry_run` setting):

```json
{
  "command": "sweep"
}
```