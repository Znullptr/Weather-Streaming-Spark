from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1,
}

with DAG(
    'start_weather_streaming_api',
    default_args=default_args,
    description='A DAG to start weather streaming application',
    schedule_interval=None,
    catchup=False
) as dag:

    start_master_zookeeper = BashOperator(
        task_id='start_master_zookeeper',
        bash_command='zkServer.sh start',
        dag=dag
    )

    start_master_kafka = BashOperator(
        task_id='start_master_kafka',
        bash_command='sudo systemctl start kafka',
        dag=dag
    )

    ssh_worker1_zookeeper = SSHOperator(
        task_id='ssh_worker1_zookeeper',
        ssh_conn_id='worker1_ssh',
        command='zkServer.sh start',
        dag=dag
    )

    ssh_worker1_kafka = SSHOperator(
        task_id='ssh_worker1_kafka',
        ssh_conn_id='worker1_ssh',  
        command='sudo systemctl start kafka',
        dag=dag
    )

    ssh_worker2_zookeeper = SSHOperator(
        task_id='ssh_worker2_zookeeper',
        ssh_conn_id='worker2_ssh', 
        command='zkServer.sh start',
        dag=dag
    )

    ssh_worker2_kafka = SSHOperator(
        task_id='ssh_worker2_kafka',
        ssh_conn_id='worker2_ssh',
        command='sudo systemctl start kafka',
        dag=dag
    )

    start_hive_metastore = BashOperator(
        task_id='start_hive_metastore',
        bash_command='sudo systemctl start hive-metastore',
        dag=dag
    )

    start_hiveserver2 = BashOperator(
        task_id='start_hiveserver2',
        bash_command='sudo systemctl start hiveserver2',
        dag=dag
    )

    run_kafka_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='source /home/hadoop/Projects/realtime_weather_application/.venv/bin/activate && /home/hadoop/Projects/realtime_weather_application/.venv/bin/python /home/hadoop/Projects/realtime_weather_application/kafka_producer.py',
        dag=dag
    )
    
    run_spark_processing = BashOperator(
    task_id='run_spark_processing',
    bash_command='sudo systemctl start weather-spark-streaming && sleep 30', # add 30 seconds for spark boot time
    dag=dag
)
    run_flask_rest_api = BashOperator(
        task_id='run_flask_rest_api',
        bash_command='source /home/hadoop/Projects/realtime_weather_application/.venv/bin/activate && /home/hadoop/Projects/realtime_weather_application/.venv/bin/python /home/hadoop/Projects/realtime_weather_application/api_app.py',
        dag=dag
    )

    run_pydash_weather_streaming_dashboard = BashOperator(
        task_id='run_pydash_weather_streaming_dashboard',
        bash_command='source /home/hadoop/Projects/realtime_weather_application/.venv/bin/activate && /home/hadoop/Projects/realtime_weather_application/.venv/bin/python /home/hadoop/Projects/realtime_weather_application/weather_dashboard.py',
        dag=dag
    )

[start_master_zookeeper, ssh_worker1_zookeeper, ssh_worker2_zookeeper] >> start_master_kafka
start_master_kafka >> [ssh_worker1_kafka, ssh_worker2_kafka]
[ssh_worker1_kafka, ssh_worker2_kafka] >> start_hive_metastore
start_hive_metastore >> start_hiveserver2
start_hiveserver2 >> run_kafka_producer >> run_flask_rest_api >> run_spark_processing
run_spark_processing >> run_pydash_weather_streaming_dashboard
