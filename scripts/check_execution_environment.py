"""Report availability of actual dependencies without falsely passing integration."""
from __future__ import annotations
import importlib.util
import json
import platform
import shutil
import socket
from pathlib import Path

DEPENDENCIES=('pyarrow','psycopg','oracledb','pyspark','airflow','confluent_kafka','sentence_transformers','boto3','pytest')

def run(report:Path)->dict:
    packages={name:importlib.util.find_spec(name) is not None for name in DEPENDENCIES}
    has_docker=shutil.which('docker') is not None
    try:
        socket.getaddrinfo('pypi.org',443)
        dns=True
    except OSError:
        dns=False
    state={'status':'ENVIRONMENT_DISCOVERY','host':platform.platform(),
           'python':platform.python_version(),'docker_binary':has_docker,
           'py_pi_dns':dns,'packages':packages,
           'real_technology_tests':'NOT_EXECUTED_IN_THIS_ENVIRONMENT',
           'external_runner_required':True}
    report.parent.mkdir(parents=True,exist_ok=True)
    report.write_text(json.dumps(state,indent=2)+'\n')
    return state

if __name__=='__main__':
    print(json.dumps(run(Path('reports/v06_runtime_discovery.json')),indent=2))
