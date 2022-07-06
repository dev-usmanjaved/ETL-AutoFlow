import os
import tarfile
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd


# Define your DAG parameters
dag_params = {
    'owner': 'John Doe',
    'start_date': datetime.today(),
    'email': 'johndoe4254@mailinator.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_tolldata',
    schedule_interval='@daily',
    default_args=dag_params,
    description='Apache Airflow Final Assessment'
)


def unzip_data():
    with tarfile.open('./data.tgz', 'r:gz') as tar:
        tar.extractall(path='unzipped_data')


unzip_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag
)


def extract_data_from_csv():
    input_path = 'unzipped_data/vehicle-data.csv'

    df = pd.read_csv(input_path, header=None)
    selected_columns = df.iloc[:, :4]
    selected_columns.columns = ['Rowid', 'Timestamp',
                                'Anonymized Vehicle number', 'Vehicle type']

    selected_columns.to_csv('csv_data.csv', index=True)


extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag
)


def extract_data_from_tsv():
    input_path = 'unzipped_data/tollplaza-data.tsv'
    df = pd.read_csv(input_path, sep='\t', header=None)
    selected_columns = df.iloc[:, -3:]
    selected_columns.columns = [
        'Number of axles', 'Tollplaza id', 'Tollplaza code']
    selected_columns.to_csv('tsv_data.csv', index=False)


extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag
)


def extract_data_from_fixed_width():
    input_path = 'unzipped_data/payment-data.txt'
    colspecs = [(58, 62), (61, 72)]
    column_names = ['Type of Payment code', 'Vehicle Code']
    df = pd.read_fwf(input_path, colspecs=colspecs,
                     header=None, names=column_names)
    df.to_csv('fixed_width_data.csv', index=False)


extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag
)

# Resolve the tilde character (~) to the user's home directory
absolute_path = os.path.expanduser('~/airflow')

consolidate_task = BashOperator(
    task_id='consolidate_data',
    bash_command=f'paste {absolute_path}/csv_data.csv {absolute_path}/tsv_data.csv {absolute_path}/fixed_width_data.csv > {absolute_path}/extracted_data.csv',
    dag=dag
)

def transform_and_save_data():
    df = pd.read_csv('extracted_data.csv', sep=',|\\t', engine='python')

    df.columns = ["", "Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type", "Number of axles", "Tollplaza id", "Tollplaza code", "Type of Payment code", "Vehicle Code"]
    df = df.drop(columns=[""])
    df['Vehicle type'] = df['Vehicle type'].str.upper()
    df.columns = df.columns.str.replace(' ', '_')
    df.to_csv('transformed_data.csv', index=False)


transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_save_data,
    dag=dag
)


unzip_task >> extract_csv_task
unzip_task >> extract_tsv_task
unzip_task >> extract_fixed_width_task
extract_csv_task >> consolidate_task
extract_tsv_task >> consolidate_task
extract_fixed_width_task >> consolidate_task
consolidate_task >> transform_task
