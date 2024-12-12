from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'titanic_pipeline',
    default_args=default_args,
    description='Titanic data processing and prediction pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define base paths
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
MODELS_DIR = os.path.join(AIRFLOW_HOME, 'models')

# Define tasks
def run_preprocessing(**context):
    from src.config.config import get_spark_session
    from src.jobs.data_preprocessing import TitanicPreprocessingJob
    
    spark = get_spark_session()
    
    preprocessing_config = {
        "input_path": os.path.join(DATA_DIR, "Titanic-Dataset.csv"),
        "output_path": os.path.join(DATA_DIR, "processed/titanic_processed"),
        "readable_output_path": os.path.join(DATA_DIR, "processed/titanic_processed_readable")
    }
    
    job = TitanicPreprocessingJob(spark, preprocessing_config)
    job.run()
    
    spark.stop()

def run_model_training(**context):
    from src.config.config import get_spark_session
    from src.jobs.model_training import TitanicModelTrainingJob
    
    spark = get_spark_session()
    
    training_config = {
        "input_path": os.path.join(DATA_DIR, "processed/titanic_processed"),
        "original_data_path": os.path.join(DATA_DIR, "Titanic-Dataset.csv"),
        "model_path": os.path.join(MODELS_DIR, "saved_models/titanic_model"),
        "predictions_path": os.path.join(DATA_DIR, "predictions/titanic_predictions")
    }
    
    job = TitanicModelTrainingJob(spark, training_config)
    job.run()
    
    spark.stop()

def export_results(**context):
    from src.config.config import get_spark_session
    from src.utils.spark_utils import export_to_csv
    
    spark = get_spark_session()
    
    # Export preprocessed data
    export_to_csv(
        spark,
        os.path.join(DATA_DIR, "processed/titanic_processed_readable"),
        os.path.join(DATA_DIR, "processed/titanic_processed.csv")
    )
    
    # Export predictions
    export_to_csv(
        spark,
        os.path.join(DATA_DIR, "predictions/titanic_predictions"),
        os.path.join(DATA_DIR, "predictions/titanic_predictions.csv")
    )
    
    spark.stop()

# Create tasks
preprocess_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=run_preprocessing,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=run_model_training,
    dag=dag,
)

export_results_task = PythonOperator(
    task_id='export_results',
    python_callable=export_results,
    dag=dag,
)

# Define task dependencies
preprocess_task >> train_model_task >> export_results_task 