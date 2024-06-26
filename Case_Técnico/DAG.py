from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator


default_args={
    'owner':'airflow',
    'depends_on_past':'False',
    'retries':1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='DAG_case',
    default_args=default_args,
    description='Dag para rodar a imagem docker todos os dias das 11h as 21h exceto nas quartas-feiras.',
    start_date= days_ago(1),
    schedule_interval='0 11-21 * * * 1,2,4,5',
    
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

run_docker = DockerOperator(
    task_id='Executar_imagem_docker',
    image='user/case_tecnico_docker',
    api_version='auto',
    auto_remove=True,
    docker_url = 'unix://var/run/docker.sock',
    network_mode = 'bridge',
    dag=dag,
)

start >> run_docker
