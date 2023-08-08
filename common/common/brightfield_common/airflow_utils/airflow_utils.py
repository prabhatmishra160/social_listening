"""
Common Functions for use in airflow DAGs

Owner: brightfield-data-eng@brightfieldgroup.com


Dependencies: 

    - Airflow
"""
import os
from airflow.models import Variable


# Read airflow variables
def get_airflow_var(var_name: str, var_type:type, nullable:bool):
    raw_variable = Variable.get(var_name)
    if nullable and (raw_variable.lower() == 'none' or raw_variable.strip() == ''):
        return None
    else:
        return var_type(raw_variable)


def get_airflow_vars(dag_name: str, var_dict: dict) -> dict:
    return_dict = {}
    for var in var_dict.keys():
        var_name = dag_name.upper()+'_'+var.upper()
        var_type = var_dict[var]['type']
        var_nullable = var_dict[var]['nullable']
        return_dict[var] = get_airflow_var(var_name, var_type, var_nullable)
    return return_dict


def get_query(dag_name: str, query_name: str) -> str:
    '''
    Gets a given .sql file for a given DAG
    '''
    home = '/home/airflow/gcs/dags'

    with open(f'{home}/sql/{dag_name.lower()}/{query_name}.sql', 'r') as f:
        query = f.read()
    
    return query
