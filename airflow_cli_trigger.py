#!/usr/bin/env python3

import argparse
import json
import requests
import sys
import time
import logging
import urllib3
import getpass
import os
from datetime import datetime
from urllib.parse import urljoin

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
def setup_logger(debug=False):
    """Setup logger configuration."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Reduce noise from requests library
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    return logging.getLogger('airflow-trigger')

class AirflowTriggerer:
    def __init__(self, airflow_url, username, password, logger):
        self.airflow_url = airflow_url.rstrip("/")
        self.username = username
        self.password = password
        self.logger = logger
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def trigger_dag(self, dag_id, conf=None):
        """Trigger a DAG run in Airflow."""
        trigger_url = urljoin(self.airflow_url, f"/api/v1/dags/{dag_id}/dagRuns")
        payload = {"conf": conf or {}}

        self.logger.info(f"Triggering DAG: {dag_id}")
        self.logger.debug(f"Trigger payload: {json.dumps(payload, indent=2)}")

        response = self.session.post(trigger_url, headers=self.headers, json=payload)
        
        if response.status_code not in (200, 201):
            self.logger.error(f"Failed to trigger DAG. HTTP {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to trigger DAG: {response.status_code}")

        data = response.json()
        dag_run_id = data.get("dag_run_id", "N/A")
        state = data.get("state", "queued")
        
        self.logger.info(f"DAG triggered successfully! Run ID: {dag_run_id}, Initial state: {state}")
        return dag_run_id

    def get_dag_run_status(self, dag_id, dag_run_id):
        """Get status of the last DAG run."""
        dag_run_url = urljoin(self.airflow_url, f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}")
        response = self.session.get(dag_run_url, headers=self.headers)

        if response.status_code != 200:
            self.logger.error(f"Error fetching DAG run status: HTTP {response.status_code}")
            self.logger.error(response.text)
            return None

        return response.json()

    def monitor_dag(self, dag_id, dag_run_id):
        """Monitor DAG run until completion."""
        self.logger.info(f"Monitoring DAG run: {dag_run_id}")
        start_time = datetime.now()
        dag_run_url = urljoin(self.airflow_url, f"/dags/{dag_id}/grid?dag_run_id={dag_run_id}")
        previous_state = None
        spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spinner_idx = 0

        terminal_states = ['success', 'failed', 'upstream_failed', 'skipped']
        
        while True:
            try:
                run_info = self.get_dag_run_status(dag_id, dag_run_id)
                if not run_info:
                    time.sleep(10)
                    continue

                state = run_info.get('state', 'unknown').lower()
                elapsed = (datetime.now() - start_time).seconds
                if state not in terminal_states:
                    sys.stdout.write(f'\r{spinner[spinner_idx % len(spinner)]} State: {state} (elapsed: {elapsed}s)')
                    sys.stdout.flush()
                    spinner_idx += 1
                else:
                    sys.stdout.write('\r' + ' ' * 80 + '\r')  # clear spinner line
                    self.logger.info(f"DAG reached terminal state: {state}")
                    elapsed_min, elapsed_sec = divmod(elapsed, 60)
                    self.logger.info(f"Total time: {elapsed_min}m {elapsed_sec}s")
                    self.logger.info(f"DAG run URL: {dag_run_url}")
                    return state

                if state != previous_state:
                    sys.stdout.write('\r' + ' ' * 80 + '\r')
                    self.logger.info(f"DAG state changed: {state}")
                    previous_state = state

            except Exception as e:
                self.logger.error(f"Error monitoring DAG: {e}")

            time.sleep(10)

def get_password():
    """Get password from environment variable, or prompt"""
    # Check environment variable
    password = os.environ.get('AIRFLOW_TRIGGER_PASSWORD')
    if password:
        logging.getLogger('airflow-trigger').debug("Using password from AIRFLOW_TRIGGER_PASSWORD environment variable")
    
    # If still not found, prompt
    if not password:
        password = getpass.getpass(f"Password for {username}: ")
    
    return password

def main():
    parser = argparse.ArgumentParser(
        description='Trigger and monitor an Airflow DAG run',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--url', required=True, help='Base URL of Airflow instance')
    parser.add_argument('--dag', required=True, help='DAG ID to trigger')
    parser.add_argument('--username', required=True, help='Airflow username')
    parser.add_argument('--password', help='Airflow password')
    parser.add_argument('--conf', help='JSON string for DAG run configuration', default="{}")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()
    logger = setup_logger(args.debug)

    # Ask for password if not provided
    if not args.password:
        args.password = get_password()
    
    try:
        conf_dict = json.loads(args.conf)
    except json.JSONDecodeError:
        logger.error("Invalid JSON provided for --conf argument.")
        sys.exit(1)

    triggerer = AirflowTriggerer(
        airflow_url=args.url,
        username=args.username,
        password=args.password,
        logger=logger
    )

    try:
        # Trigger DAG and check its status on successful trigger
        dag_run_id = triggerer.trigger_dag(args.dag, conf_dict)
        final_state = triggerer.monitor_dag(args.dag, dag_run_id)
        
        if final_state == 'success':
            logger.info("DAG completed successfully!")
        elif final_state in ['failed', 'upstream_failed']:
            logger.error("DAG failed!")
            sys.exit(1)
        else:
            logger.warning(f"DAG ended with unexpected state: {final_state}")
            sys.exit(2)

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
