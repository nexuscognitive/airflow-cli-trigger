#!/usr/bin/env python3

import argparse
import json
import requests
import sys
import time
import logging
import urllib3
import getpass
import yaml
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
    def __init__(self, airflow_url, username, password, logger, airflow_version=None):
        self.airflow_url = airflow_url.rstrip("/")
        self.username = username
        self.password = password
        self.logger = logger
        self.session = requests.Session()
        self.session.verify = False
        if airflow_version is None:
            self.airflow_version = self._detect_airflow_version()
        else:
            self.airflow_version = airflow_version
            
        self.logger.info(f"Using Airflow version: {self.airflow_version}")
        
        # Set up authentication based on version
        if self.airflow_version == 3:
            self._setup_airflow3_auth()
        else:
            self._setup_airflow2_auth()

    def _detect_airflow_version(self):
        """
        Auto-detect Airflow version by trying /v2/version endpoint with basic auth
        """
        self.logger.info("Auto-detecting Airflow version...")
        
        version_url = urljoin(self.airflow_url, "/api/v2/version")
        
        try:
            # Try with basic auth first
            response = requests.get(
                version_url,
                auth=(self.username, self.password),
                verify=False,
                timeout=10
            )
            
            if response.status_code == 404:
                # v2 API doesn't exist = Airflow 2
                self.logger.info("Detected Airflow 2 (v2 API not found)")
                return 2
            elif response.status_code == 200:
                # v2 API exists = Airflow 3
                self.logger.info("Detected Airflow 3 (v2 API found)")
                return 3
            elif response.status_code == 401:
                # v2 API exists but needs token → Airflow 3
                self.logger.info("Detected Airflow 3 (v2 API found)")
                return 3
            else:
                # Default to Airflow 2 for unexpected responses
                self.logger.warning(f"Unexpected response code {response.status_code}, defaulting to Airflow 2")
                return 2
                
        except Exception as e:
            self.logger.warning(f"Error detecting version: {e}, defaulting to Airflow 2")
            return 2
    
    def _setup_airflow2_auth(self):
        """Setup authentication for Airflow 2 (Basic Auth)."""
        self.session.auth = (self.username, self.password)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.api_version = "v1"  # Use v1 API for Airflow 2
    
    def _setup_airflow3_auth(self):
        """Setup authentication for Airflow 3 (Token-based Auth)."""
        self.logger.info("Authenticating with Airflow 3...")
        
        # Get access token from /auth/token endpoint
        token_url = urljoin(self.airflow_url, "/auth/token")
        payload = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(
                token_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                verify=False,
                timeout=10
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to authenticate with Airflow 3. HTTP {response.status_code}")
                self.logger.error(response.text)
                raise Exception(f"Authentication failed: {response.status_code}")
            
            token_data = response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise Exception("No access token in response")
            
            # Set up session with Bearer token
            self.session.auth = None  # Remove basic auth
            self.headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}"  # Add Bearer token
            }
            self.api_version = "v2"  # Use v2 API for Airflow 3
            
            self.logger.info("Successfully authenticated with Airflow 3")
            
        except Exception as e:
            self.logger.error(f"Error during Airflow 3 authentication: {e}")
            raise

    def trigger_dag(self, dag_id, conf=None):
        """Trigger a DAG run in Airflow."""
        trigger_url = urljoin(self.airflow_url, f"/api/{self.api_version}/dags/{dag_id}/dagRuns")
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

        modified_dag_run_id = dag_run_id.replace(':', '%3A').replace('+', '%2B')
        
        dag_run_url = urljoin(self.airflow_url, f"/dags/{dag_id}/grid?dag_run_id={modified_dag_run_id}")
        
        self.logger.info(f"DAG triggered successfully! Run ID: {dag_run_id}, Initial state: {state}")
        self.logger.info(f"DAG run URL: {dag_run_url}")
        return dag_run_id

    def get_dag_run_status(self, dag_id, dag_run_id):
        """Get status of the last DAG run."""
        dag_run_url = urljoin(self.airflow_url, f"/api/{self.api_version}/dags/{dag_id}/dagRuns/{dag_run_id}")
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
                    return state

                if state != previous_state:
                    sys.stdout.write('\r' + ' ' * 80 + '\r')
                    self.logger.info(f"DAG state changed: {state}")
                    previous_state = state

            except Exception as e:
                self.logger.error(f"Error monitoring DAG: {e}")

            time.sleep(10)

def get_password(config, username):
    """Get password from environment variable, or prompt"""
    # First check if password is in config
    password = config.get('password')
    
    # Else check environment variable
    if not password:
        password = os.environ.get('AIRFLOW_TRIGGER_PASSWORD')
        if password:
            logging.getLogger('airflow-trigger').debug("Using password from AIRFLOW_TRIGGER_PASSWORD environment variable")
    
    # If still not found, prompt
    if not password:
        password = getpass.getpass(f"Password for {username}: ")
    
    return password

def load_yaml_config(config_file):
    """Load configuration from YAML file."""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def merge_configs(yaml_config, cli_args):
    """Merge YAML config with CLI arguments. CLI arguments take precedence."""
    merged = {}
    
    # Start with YAML config if provided
    if yaml_config:
        merged = yaml_config.copy()
    
    # Override with CLI arguments (only if they are explicitly provided)
    cli_dict = vars(cli_args)
    for key, value in cli_dict.items():
        # Skip None values and special keys
        if value is not None and key not in ['config_file', 'func']:
            # For boolean flags, only override if True (explicitly set)
            if isinstance(value, bool):
                if value or key not in merged:
                    merged[key] = value
            else:
                merged[key] = value
    
    return merged

def main():
    parser = argparse.ArgumentParser(
        description='Trigger and monitor an Airflow DAG run',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--url', help='Base URL of Airflow instance')
    parser.add_argument('--dag', help='DAG ID to trigger')
    parser.add_argument('--username', help='Airflow username')
    parser.add_argument('--password', help='Airflow password')
    parser.add_argument('--conf', help='JSON string for DAG run configuration', default="{}")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--config-file', help='YAML configuration file')
    parser.add_argument('--airflow-version', type=int, choices=[2, 3], 
                       help='Airflow version (2 or 3). Auto-detects if not specified.')

    args = parser.parse_args()

    # Load YAML config if provided
    yaml_config = None
    if args.config_file:
        try:
            yaml_config = load_yaml_config(args.config_file)
        except Exception as e:
            print(f"Error loading config file: {e}")
            sys.exit(1)
    
    # Merge configurations
    config = merge_configs(yaml_config, args)

    # Validate required fields
    required_fields = ['url', 'dag', 'username']
    missing_fields = [field for field in required_fields if field not in config or not config[field]]
    if missing_fields:
        print(f"Error: Missing required fields: {', '.join(missing_fields)}")
        print("These must be provided either in the config file or as command-line arguments")
        sys.exit(1)

    logger = setup_logger(args.debug)

    if args.config_file:
        logger.info(f"Loaded configuration from: {args.config_file}")
        
    password = get_password(config, config['username'])
    
    try:
        conf_dict = json.loads(args.conf)
    except json.JSONDecodeError:
        logger.error("Invalid JSON provided for --conf argument.")
        sys.exit(1)

    airflow_version = config.get('airflow_version') or args.airflow_version

    triggerer = AirflowTriggerer(
        airflow_url=config['url'], 
        username=config['username'], 
        password=password, 
        logger=logger,
        airflow_version=airflow_version
    )

    try:
        # Trigger DAG and check its status on successful trigger
        dag_run_id = triggerer.trigger_dag(config['dag'], conf_dict)
        final_state = triggerer.monitor_dag(config['dag'], dag_run_id)
        
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
