# Airflow CLI Trigger
A powerful Python CLI tool to trigger and monitor the Airflow DAGs.

## Features

- ✅ **Trigger Airflow DAGs** 
- ✅ **Real-time joacb monitoring** with status updates and progress spinner
- ✅ **Flexible authentication** with password from CLI, environment variable, or interactive prompt
- ✅ **Configurable DAG properties** via CLI

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
    --username <user_mail-id> \
    --password <user_password> \
    --dag <dag_name> \
    --debug
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

2. **Environment Variable**
   ```bash
   export AIRFLOW_TRIGGER_PASSWORD='my-password'
   python airflow_cli_trigger.py ...
   ```

3. **Interactive Prompt** (most secure)
   ```
   Password for username: [hidden input]
   ```

## Output and Logging

### Standard Output

```
2025-11-05 20:09:20 - airflow-trigger - INFO - Triggering DAG: trigger_d
2025-11-05 20:09:23 - airflow-trigger - INFO - DAG triggered successfully! Run ID: manual__2025-11-05T20:09:23.161858+00:00, Initial state: queued
2025-11-05 20:09:23 - airflow-trigger - INFO - Monitoring DAG run: manual__2025-11-05T20:09:23.161858+00:00
2025-11-05 20:09:23 - airflow-trigger - INFO - DAG state changed: queued        
2025-11-05 20:09:33 - airflow-trigger - INFO - DAG state changed: running       
2025-11-05 20:10:55 - airflow-trigger - INFO - DAG reached terminal state: success
2025-11-05 20:10:55 - airflow-trigger - INFO - Total time: 1m 32s
2025-11-05 20:10:55 - airflow-trigger - INFO - DAG run URL: https://airflow.rapid.nx1cloud.com/dags/trigger_d/grid?dag_run_id=manual__2025-11-05T20:09:23.161858+00:00
2025-11-05 20:10:55 - airflow-trigger - INFO - DAG completed successfully!
```
