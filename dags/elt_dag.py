from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

from scripts.make_elt_table import *


with DAG(
    dag_id = 'makeELTtable',
    start_date = datetime(2025, 1, 31),
    catchup=False,
    tags = ['third-project'],
    schedule='11 30 * * *' 
) as dag: 
    
    #크롤링 DAG 끝났는지 확인 
    check_airflow_dag = ExternalTaskSensor(
        task_id = 'check_airflow_end',
        external_dag_id='dataCrawlingTest',
        external_task_id='datawarehouse_upload',
        timeout=1800,
        mode='reschedule'
    )
    
    #환율 DAG 끝났는지 확인
    check_exchange_dag = ExternalTaskSensor(
        task_id = 'check_exchange_end',
        external_dag_id='currencyDataETL',
        external_task_id='load_snowflake',
        timeout=600,
        mode='reschedule'
    )
    
    #날씨 DAG 끝났는지 확인
    check_weather_dag = ExternalTaskSensor(
        task_id = 'check_weather_end',
        external_dag_id='get_city_weather',
        external_task_id='load',
        timeout=600,
        mode='reschedule'
    )
    
    #최종 테이블 생성 
    make_info_table = PythonOperator(
        task_id = 'bucket_upload',
        python_callable= daily_info_table,
    )
    
    
    [check_airflow_dag, check_exchange_dag, check_weather_dag] >> make_info_table
    