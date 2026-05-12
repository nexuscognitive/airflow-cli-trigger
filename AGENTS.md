# Airflow CLI Trigger

Python CLI tool to trigger and monitor Apache Airflow DAG runs with real-time status tracking.

## Commands

```bash
# Trigger and monitor a DAG
python airflow_cli_trigger.py \
    --url https://airflow.example.com \
    --username <user> --password <pass> \
    --dag <dag_id>

# Use a YAML config file instead
python airflow_cli_trigger.py --config-file dag_config.yaml
```

## Setup

```bash
python -m venv venv && source venv/bin/activate
pip install requests pyyaml
```

## Key Details

- Password resolution order: CLI flag -> YAML config -> `AIRFLOW_TRIGGER_PASSWORD` env var -> interactive prompt
- The tool polls the Airflow REST API every 10 seconds until the DAG reaches a terminal state
- Exits 0 on success, 1 on failure, 2 on unexpected terminal state
