"""
Exercise 17: BigQuery ETL Pipeline
Author: Sherif Elashmawy
Date: January 2026

This DAG demonstrates a complete ETL pipeline with BigQuery:
1. Extract: Query public BigQuery data
2. Transform: Process data locally with Python/pandas
3. Load: Upload transformed data back to BigQuery
4. Transform in BigQuery: SQL-based transformations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
from google.cloud import bigquery
import json

# Configuration
PROJECT_ID = 'data-analytics-project-482302'
DATASET_ID = 'airflow_etl_demo'
TABLE_ID = 'usa_names_analysis'
TEMP_TABLE_ID = 'usa_names_temp'

default_args = {
    'owner': 'sherif',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bigquery_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline with BigQuery - Extract, Transform, Load',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['bigquery', 'etl', 'exercise-17'],
)

def extract_from_bigquery(**context):
    """
    Task 1: Extract data from BigQuery public dataset
    Query USA names data and push to XCom
    """
    print("="*60)
    print("STEP 1: EXTRACTING DATA FROM BIGQUERY")
    print("="*60)
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Query public USA names dataset
    query = """
        SELECT 
            name,
            gender,
            SUM(number) as total_count,
            COUNT(DISTINCT year) as years_appeared,
            MIN(year) as first_year,
            MAX(year) as last_year
        FROM `bigquery-public-data.usa_names.usa_1910_current`
        WHERE year >= 2000
        GROUP BY name, gender
        HAVING total_count > 10000
        ORDER BY total_count DESC
        LIMIT 100
    """
    
    print(f"\nExecuting query...")
    print(f"Source: bigquery-public-data.usa_names.usa_1910_current")
    print(f"Filter: Years >= 2000, Total count > 10,000")
    print(f"Limit: Top 100 names")
    
    df = client.query(query).to_dataframe()
    
    print(f"\n✓ Extracted {len(df)} records")
    print(f"✓ Columns: {list(df.columns)}")
    print(f"\nSample data:")
    print(df.head())
    
    # Convert to JSON for XCom
    data_json = df.to_json(orient='records')
    
    # Push to XCom for next task
    context['task_instance'].xcom_push(key='extracted_data', value=data_json)
    
    print(f"\n✓ Data pushed to XCom")
    print("="*60)
    
    return f"Extracted {len(df)} records"

def transform_data(**context):
    """
    Task 2: Transform data locally with Python/pandas
    Add calculated columns and enrich the data
    """
    print("="*60)
    print("STEP 2: TRANSFORMING DATA WITH PYTHON/PANDAS")
    print("="*60)
    
    # Pull data from XCom
    data_json = context['task_instance'].xcom_pull(
        task_ids='extract_from_bigquery',
        key='extracted_data'
    )
    
    df = pd.read_json(data_json)
    
    print(f"\n✓ Loaded {len(df)} records from XCom")
    
    # Transformation 1: Calculate popularity score
    print("\n1. Calculating popularity score...")
    df['popularity_score'] = (
        df['total_count'] / df['years_appeared']
    ).round(2)
    
    # Transformation 2: Add decade category
    print("2. Adding decade category...")
    df['first_decade'] = (df['first_year'] // 10) * 10
    
    # Transformation 3: Calculate name length
    print("3. Calculating name length...")
    df['name_length'] = df['name'].str.len()
    
    # Transformation 4: Add popularity tier
    print("4. Categorizing into popularity tiers...")
    df['popularity_tier'] = pd.cut(
        df['total_count'],
        bins=[0, 50000, 100000, float('inf')],
        labels=['Medium', 'High', 'Very High']
    )
    
    # Transformation 5: Add processing timestamp
    print("5. Adding processing metadata...")
    df['processed_at'] = datetime.now().isoformat()
    df['pipeline_version'] = '1.0'
    
    print(f"\n✓ Transformations completed")
    print(f"✓ New columns added: {['popularity_score', 'first_decade', 'name_length', 'popularity_tier', 'processed_at', 'pipeline_version']}")
    print(f"\nTransformed data sample:")
    print(df[['name', 'total_count', 'popularity_score', 'popularity_tier']].head())
    
    # Convert back to JSON
    transformed_json = df.to_json(orient='records')
    context['task_instance'].xcom_push(key='transformed_data', value=transformed_json)
    
    print(f"\n✓ Transformed data pushed to XCom")
    print("="*60)
    
    return f"Transformed {len(df)} records with 6 new columns"

def load_to_bigquery(**context):
    """
    Task 3: Load transformed data back to BigQuery
    """
    print("="*60)
    print("STEP 3: LOADING DATA TO BIGQUERY")
    print("="*60)
    
    # Pull transformed data
    data_json = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data'
    )
    
    df = pd.read_json(data_json)
    
    print(f"\n✓ Loaded {len(df)} transformed records from XCom")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    # Create dataset if not exists
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        print(f"✓ Dataset {DATASET_ID} already exists")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✓ Created dataset {DATASET_ID}")
    
    # Define table schema
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Load data to BigQuery
    print(f"\nUploading to: {table_id}")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    
    job.result()  # Wait for completion
    
    table = client.get_table(table_id)
    print(f"\n✓ Loaded {table.num_rows} rows to BigQuery")
    print(f"✓ Table: {table_id}")
    print(f"✓ Columns: {len(table.schema)}")
    
    print("="*60)
    
    return f"Loaded {table.num_rows} rows to {table_id}"

# Define tasks
task_extract = PythonOperator(
    task_id='extract_from_bigquery',
    python_callable=extract_from_bigquery,
    provide_context=True,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    provide_context=True,
    dag=dag,
)

# BigQuery SQL transformation task
task_sql_transform = BigQueryInsertJobOperator(
    task_id='transform_in_bigquery',
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_summary` AS
                SELECT 
                    gender,
                    popularity_tier,
                    COUNT(*) as name_count,
                    AVG(total_count) as avg_total_count,
                    AVG(popularity_score) as avg_popularity_score,
                    AVG(name_length) as avg_name_length,
                    MIN(first_year) as earliest_first_year,
                    MAX(last_year) as latest_last_year
                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                GROUP BY gender, popularity_tier
                ORDER BY gender, popularity_tier
            """,
            "useLegacySql": False,
        }
    },
    location='US',
    dag=dag,
)

# Task dependencies - ETL Pipeline
task_extract >> task_transform >> task_load >> task_sql_transform
