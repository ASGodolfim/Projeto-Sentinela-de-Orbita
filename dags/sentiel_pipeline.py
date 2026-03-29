from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import csv
import json
import glob
import io
import os
import shutil

args_default={
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def validate_data():
    json_files = glob.glob("include/inbound/telemetry_*.json")
    csv_files = glob.glob("include/inbound/position_*.csv")

    if not json_files or not csv_files:
        raise ValueError("Não foram encontrados arquivos JSON ou CSV")
    
    latest_json = json_files[-1]
    latest_csv = csv_files[-1]

    try:
        with open(latest_json, 'r') as f:
            data_json = json.load(f)
    except Exception as e:
        raise ValueError(f"Erro ao validar JSON: {e}")
    
    try:
        with open(latest_csv, 'r') as f:
            data_csv = csv.reader(f)
            required_columns = ["satellite_id", "latitude", "longitude", "altitude_km", "timestamp"]
            header = next(data_csv)

            if header != required_columns:
                raise ValueError(f"Colunas CSV inválidas. Encontradas: {header}")
            else:
                print("Colunas do CSV válidas")
    except Exception as e:
        raise ValueError(f"Erro ao validar CSV: {e}")


def csv_filter():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    csv_files = glob.glob("include/inbound/position_*.csv")
    
    for csv_file in csv_files:
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("CREATE TEMP TABLE stg_temp_csv (LIKE stg_satellite_position);")
        
        sql_copy = "COPY stg_temp_csv FROM STDIN WITH CSV HEADER DELIMITER ','"
        with open(csv_file, 'r') as f:
            cursor.copy_expert(sql_copy, f)
    
        cursor.execute("""
        INSERT INTO stg_satellite_position
        SELECT * FROM stg_temp_csv
        ON CONFLICT (satellite_id, timestamp) DO NOTHING;
        """)
        cursor.execute("DROP TABLE stg_temp_csv;")
        
        conn.commit()
        cursor.close()
        conn.close()

def json_filter():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    json_files = glob.glob("include/inbound/telemetry_*.json")
    
    for json_file in json_files:
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("CREATE TEMP TABLE stg_temp_json (LIKE stg_telemetry_battery);")
        
        with open(json_file, 'r') as f:
            data = json.load(f)
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            for record in data:
                writer.writerow([
                    record.get('satellite_id', ''),
                    record.get('voltage', ''),
                    record.get('temperature_c', ''),
                    record.get('timestamp', '')
                ])
            buffer.seek(0)
            
            sql_copy = "COPY stg_temp_json FROM STDIN WITH CSV"
            cursor.copy_expert(sql_copy, buffer)
    
        cursor.execute("""
        INSERT INTO stg_telemetry_battery
        SELECT * FROM stg_temp_json
        ON CONFLICT (satellite_id, timestamp) DO NOTHING;
        """)
        cursor.execute("DROP TABLE stg_temp_json;")
        
        conn.commit()
        cursor.close()
        conn.close()

def archive_files():
    json_files = glob.glob("include/inbound/telemetry_*.json")
    csv_files = glob.glob("include/inbound/position_*.csv")
    
    today_str = datetime.now().strftime("data=%Y-%m-%d")
    archive_dir = os.path.join("include/processed", today_str)
    
    os.makedirs(archive_dir, exist_ok=True)
    
    for path in json_files + csv_files:
        filename = os.path.basename(path)
        shutil.move(path, os.path.join(archive_dir, filename))


with DAG(
    dag_id="sentiel_pipeline",
    start_date=datetime(2026, 3, 28),
    schedule="@daily",
    default_args=args_default,
    catchup=False,
    template_searchpath=["/usr/local/airflow/"]
) as dag:
    def check_telemetry():
        return len(glob.glob("include/inbound/telemetry_*.json")) > 0

    def check_position():
        return len(glob.glob("include/inbound/position_*.csv")) > 0

    wait_for_telemetry = PythonSensor(
        task_id='wait_for_telemetry',
        python_callable=check_telemetry,
        poke_interval=10,
        timeout=60*60*24,
        mode='poke'
    )
        
    wait_for_position = PythonSensor(
        task_id='wait_for_position',
        python_callable=check_position,
        poke_interval=10,
        timeout=60*60*24,
        mode='poke'
    )
    
    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    init_database_task = SQLExecuteQueryOperator(
        task_id="init_database",
        conn_id="postgres_default",
        sql="include/sql/init_tables.sql"
    )

    csv_filter_task = PythonOperator(
        task_id="csv_filter",
        python_callable=csv_filter,
    )

    json_filter_task = PythonOperator(
        task_id="json_filter",
        python_callable=json_filter,
    )

    archive_task = PythonOperator(
        task_id="archive_files",
        python_callable=archive_files,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_transformation",
        bash_command="dbt run --project-dir /usr/local/airflow/include/dbt/sentinela --profiles-dir /usr/local/airflow/include/dbt/sentinela",
    )

    [wait_for_telemetry, wait_for_position] >> validate_data_task >> init_database_task >> [csv_filter_task, json_filter_task] >> archive_task >> dbt_run_task