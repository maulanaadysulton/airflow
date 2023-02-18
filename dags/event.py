import json
import csv
import time
import glob
import re
import datetime
import pandas as pd
import os, stat
from pathlib import Path
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
import datetime
from dateutil.relativedelta import relativedelta


# files = glob.glob("./raw_data/2022-10-01/*.json")    # filter folder based on running date
# suffix_filenames = [int(Path(file).stem.split('-')[1]) for file in files]
# start_time = {{execution_date.strftime('%H%M%S')}}
# end_time = {{next_execution_date.strftime('%H%M%S')}}
# filtered_filenames = list(filter(lambda x: start_time <= x <= end_time, suffix_filenames))     # between timestamp start and timestamp end
# json_files = ['D:/JALA_Tech/assessment/raw_data/2022-10-01/20221001-'+str(filtered_filename).zfill(6)+'.json' for filtered_filename in filtered_filenames]


def _validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

def _find_nth(text, pattern, n):
    start = text.find(pattern)
    while start >= 0 and n > 1:
        start = text.find(pattern, start+len(pattern))
        n -= 1
    return start   

def _get_data(dest_path, execution_date, next_execution_date, **context):
    records = list()
    time_change = datetime.timedelta(hours=7)
    start_back_date = execution_date + relativedelta(months=-4) + time_change
    end_back_date = next_execution_date + relativedelta(months=-4) + time_change
    files = glob.glob("/opt/airflow/data/{}/*.json".format(start_back_date.strftime('%Y-%m-%d')))    # filter folder based on running date
    suffix_filenames = [int(Path(file).stem.split('-')[1]) for file in files]
    start_time = int((start_back_date).strftime('%H%M%S')) 
    end_time = int((end_back_date).strftime('%H%M%S')) 
    filtered_filenames = list(filter(lambda x: start_time <= x <= end_time, suffix_filenames))  # filter files based on execution time
    json_files = ['/opt/airflow/data/{0}/{1}-'.format(start_back_date.strftime('%Y-%m-%d'), start_back_date.strftime('%Y%m%d'))+str(filtered_filename).zfill(6)+'.json' for filtered_filename in filtered_filenames]

    for file in json_files:
        with open(file, "r") as f:
            x = f.read()
            if _validateJSON(x) == True:
                data = json.loads(x)
                headers = data[0].keys()
                for i in range(len(data)):
                    record = list((data[i].values()))
                    records.append(record)
            else:
                print(file, 'is invalid json file, try to fix it.\n')
                prefix = len(re.findall('\[{',x))
                suffix = len(re.findall('\}]',x))
                if prefix != suffix:
                    text = _find_nth(x, "}]", prefix)
                    text = (x[0:text+2:1])
                    data = json.loads(text)
                    headers = data[0].keys()
                    for i in range(len(data)):
                        record = list((data[i].values()))
                        records.append(record)
                    print('file fixed.\n')

    dest_path = dest_path+'result_{}.csv'.format((start_back_date).strftime('%Y-%m-%d_%H%M%S'))                                   
    with open(dest_path,'w+', newline='') as wf:
        writer = csv.writer(wf)
        writer.writerow(headers)
        for i in range(len(records)):
            writer.writerow(records[i])   
    df = pd.read_csv(dest_path) 
    df.to_csv(dest_path, sep = '^', index=False)    
    # os.chmod(dest_path, stat.S_IRWXO)
    return dest_path            


def bulk_load_sql(table_name, separator, **context):
    local_filepath = context['ti'].xcom_pull(task_ids='get_data')
    conn = MySqlHook(conn_name_attr='mysql_default').get_conn()
    cur = conn.cursor()
    cur.execute("""
        LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table} FIELDS TERMINATED BY '{sep}'
            IGNORE 1 ROWS
    """.format(tmp_file=local_filepath, table=table_name, sep=separator))
    conn.commit()

    # conn.bulk_load(table_name, local_filepath)

dag = DAG(
    dag_id="get_event_data",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=datetime.timedelta(hours=2),
    catchup=False
)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        'dest_path': "/opt/airflow/data/",
    },
    dag=dag,
)

load_to_mysql = PythonOperator(
        task_id='load_to_mysql',
        python_callable=bulk_load_sql,
        op_kwargs={'table_name': 'jala.stg_event', 'separator':'^'},
        dag=dag)

remove_file = BashOperator(
task_id="remove_file",
bash_command="sudo rm -r {{ti.xcom_pull(task_ids='get_data')}}",
dag=dag)


fnc_dim_region_sql = MySqlOperator(
        task_id="fnc_dim_region_sql", sql="""SELECT fnc_dim_region(int(execution_date.strftime('%Y%m%d')));""", dag=dag
    )

fnc_fact_event_sql = MySqlOperator(
        task_id="fnc_fact_event_sql", sql="""SELECT fnc_fact_event(int(execution_date.strftime('%Y%m%d')));""", dag=dag
    )    

truncate_stg = MySqlOperator(
        task_id="truncate_stg", sql=r"""TRUNCATE TABLE jala.stg_event;""", dag=dag
    )    

get_data >> load_to_mysql >> fnc_dim_region_sql >> fnc_fact_event_sql >> truncate_stg >> remove_file
