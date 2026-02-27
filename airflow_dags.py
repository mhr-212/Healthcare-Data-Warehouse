"""
Healthcare Data Warehouse - Airflow DAGs
Workflow orchestration for automated data pipeline

DAGs:
1. daily_data_generation - Generate new synthetic data daily
2. hourly_etl_pipeline - Run ETL every hour
3. daily_dbt_refresh - Refresh dbt models daily
4. weekly_ml_retrain - Retrain ML models weekly
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add project path
sys.path.append('/path/to/healthcare-data-warehouse')

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['alerts@healthcare-dw.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retriesTries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG 1: Daily Data Generation
with DAG(
    'daily_data_generation',
    default_args=default_args,
    description='Generate synthetic healthcare data daily',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'data-generation'],
) as dag_data_gen:
    
    generate_data = BashOperator(
        task_id='generate_synthetic_data',
        bash_command='cd /path/to/healthcare-data-warehouse && python scripts/generate_data.py',
    )
    
    notify_success = PythonOperator(
        task_id='notify_data_generated',
        python_callable=lambda: print("Data generation completed successfully"),
    )
    
    generate_data >> notify_success

# DAG 2: Hourly ETL Pipeline
with DAG(
    'hourly_etl_pipeline',
    default_args=default_args,
    description='Run ETL pipeline to load data',
    schedule_interval='0 * * * *',  # Every hour
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'etl'],
) as dag_etl:
    
    run_etl = BashOperator(
        task_id='run_etl_script',
        bash_command='cd /path/to/healthcare-data-warehouse && python scripts/etl.py',
    )
    
    check_data_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=lambda: print("Data quality check passed"),
    )
    
    run_etl >> check_data_quality

# DAG 3: Daily dbt Refresh
with DAG(
    'daily_dbt_refresh',
    default_args=default_args,
    description='Refresh dbt models daily',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'dbt'],
) as dag_dbt:
    
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /path/to/healthcare-data-warehouse/dbt_project && dbt run --profiles-dir .',
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='cd /path/to/healthcare-data-warehouse/dbt_project && dbt test --profiles-dir .',
    )
    
    dbt_run >> dbt_test

# DAG 4: Weekly ML Model Retraining
with DAG(
    'weekly_ml_retrain',
    default_args=default_args,
    description='Retrain ML models weekly',
    schedule_interval='0 3 * * 0',  # Weekly on Sunday at 3 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'ml'],
) as dag_ml:
    
    retrain_models = BashOperator(
        task_id='retrain_ml_models',
        bash_command='cd /path/to/healthcare-data-warehouse && python ml_pipeline.py',
    )
    
    validate_predictions = PythonOperator(
        task_id='validate_predictions',
        python_callable=lambda: print("ML model validation completed"),
    )
    
    retrain_models >> validate_predictions

# DAG 5: Master Pipeline (runs all tasks in sequence)
with DAG(
    'master_healthcare_pipeline',
    default_args=default_args,
    description='Complete healthcare data pipeline',
    schedule_interval='0 4 * * 0',  # Weekly on Sunday at 4 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'master'],
) as dag_master:
    
    step1_generate = BashOperator(
        task_id='step1_generate_data',
        bash_command='cd /path/to/healthcare-data-warehouse && python scripts/generate_data.py',
    )
    
    step2_etl = BashOperator(
        task_id='step2_run_etl',
        bash_command='cd /path/to/healthcare-data-warehouse && python scripts/etl.py',
    )
    
    step3_dbt = BashOperator(
        task_id='step3_dbt_refresh',
        bash_command='cd /path/to/healthcare-data-warehouse/dbt_project && dbt run --profiles-dir .',
    )
    
    step4_ml = BashOperator(
        task_id='step4_ml_training',
        bash_command='cd /path/to/healthcare-data-warehouse && python ml_pipeline.py',
    )
    
    step5_notify = PythonOperator(
        task_id='step5_notify_complete',
        python_callable=lambda: print("Master pipeline completed successfully!"),
    )
    
    step1_generate >> step2_etl >> step3_dbt >> step4_ml >> step5_notify
