# Airflow CLI Trigger
A powerful Python CLI tool to trigger and monitor the Airflow DAGs.

## Features

- ✅ **Trigger Airflow DAGs** 
- ✅ **Real-time DAG monitoring** with status updates and progress spinner
- ✅ **Flexible authentication** with password from CLI, YAML, environment variable, or interactive prompt
- ✅ **Configurable DAG properties** via CLI and YAML

## Installation

### Prerequisites

- Python 3.6+
- Pip package manager

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/nexuscognitive/airflow-cli-trigger.git
cd airflow-cli-trigger

# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

**requirements.txt:**
```txt
certifi==2025.10.5
charset-normalizer==3.4.4
idna==3.11
PyYAML==6.0.3
requests==2.32.5
urllib3==2.5.0
```

### Make Script Executable

```bash
chmod +x airflow_cli_trigger.py
```

## Quick Start

### Basic triggering of Airflow DAG
```bash
python airflow_cli_trigger.py \
    --url airflow.example.com \
    --username <user> \
    --password <user_password> \
    --dag <dag_name> \
    --debug
```

### Using YAML Configuration

Create a configuration file `dag_config.yaml`:
```yaml
url: https://airflow.example.com
username: user
password: my-password
debug: false
dag: dag_name
```

Run with:
```bash
python airflow_cli_trigger.py --config-file dag_config.yaml
```

## Configuration

### Command Line Options

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `--url` | Yes | Airflow server URL | `https://airflow.example.com` |
| `--dag` | Yes | Dag ID | `example_dag` |
| `--username` | Yes | Authentication username | `user` |
| `--password` | No | Authentication password | `my-password` |
| `--conf` | No | JSON string airflow DAG configurations | `'{"param1": value1, "param2": value2}'` |
| `--debug` | No | Enable debug logging | Flag only |
| `--config-file` | No | YAML configuration file path | `config/dag_config.yaml` |

### YAML Configuration

YAML files can contain all configuration options. CLI arguments override YAML values.

#### Basic Structure
```yaml
url: https://airflow.example.com
username: user
password: my-password
debug: false
dag: dag_name

# DAG configuration (as dictionary)
conf:
  param1: value1
  param2: value2
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `AIRFLOW_TRIGGER_PASSWORD` | Default password for authentication |

## Password Management

The tool supports multiple methods for password authentication, in order of precedence:

1. **Command Line Argument** (highest priority)
   ```bash
   python airflow_cli_trigger.py --password 'my-password' ...
   ```

2. **YAML Configuration File**
   ```yaml
   password: my-password
   ```

3. **Environment Variable**
   ```bash
   export AIRFLOW_TRIGGER_PASSWORD='my-password'
   python airflow_cli_trigger.py ...
   ```

4. **Interactive Prompt** (most secure)
   ```
   Password for username: [hidden input]
   ```

## Output and Logging

### Standard Output

```
2025-11-06 16:34:41 - airflow-trigger - INFO - Triggering DAG: trigger_d
2025-11-06 16:34:44 - airflow-trigger - INFO - DAG triggered successfully! Run ID: manual__2025-11-06T16:34:43.862096+00:00, Initial state: queued
2025-11-06 16:34:44 - airflow-trigger - INFO - DAG run URL: https://airflow.rapid.nx1cloud.com/dags/trigger_d/grid?dag_run_id=manual__2025-11-06T16%3A34%3A43.862096%2B00%3A00
2025-11-06 16:34:44 - airflow-trigger - INFO - Monitoring DAG run: manual__2025-11-06T16:34:43.862096+00:00
2025-11-06 16:34:44 - airflow-trigger - INFO - DAG state changed: queued        
2025-11-06 16:34:54 - airflow-trigger - INFO - DAG state changed: running       
2025-11-06 16:37:36 - airflow-trigger - INFO - DAG reached terminal state: success
2025-11-06 16:37:36 - airflow-trigger - INFO - Total time: 2m 52s
2025-11-06 16:37:36 - airflow-trigger - INFO - DAG completed successfully!
```
