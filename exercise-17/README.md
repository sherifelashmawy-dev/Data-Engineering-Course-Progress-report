# Exercise 17: BigQuery ETL Pipeline with Airflow

## Objective
Create a production-grade ETL pipeline in Apache Airflow that demonstrates the complete data engineering workflow with BigQuery:
1. **Extract**: Query public BigQuery data
2. **Transform**: Process data locally with Python/pandas
3. **Load**: Upload transformed data back to BigQuery
4. **Transform in BigQuery**: SQL-based aggregations

## Important Note

The actual DAG file for this exercise is located in:
```
exercise-16/dags/bigquery_etl_pipeline.py
```

This is because Airflow (Exercise 16) serves as the infrastructure for running Exercise 17's ETL pipeline.

## Pipeline Architecture
```
Extract (BigQuery) → Transform (Python/pandas) → Load (BigQuery) → Transform (SQL)
```

**4 Tasks**:
1. `extract_from_bigquery` - Query USA names public data
2. `transform_data` - Add calculated columns with pandas
3. `load_to_bigquery` - Upload to BigQuery table
4. `transform_in_bigquery` - SQL aggregations

## Configuration
- **DAG**: `bigquery_etl_pipeline` 
- **Project**: data-analytics-project-482302
- **Dataset**: airflow_etl_demo
- **Tables**: usa_names_analysis, usa_names_analysis_summary

## Running the Pipeline

1. Access Airflow: http://localhost:8081
2. Find `bigquery_etl_pipeline` DAG
3. Unpause and trigger
4. All tasks should complete successfully (green)

## Results

**Table 1**: `usa_names_analysis` (~100 rows)
- Enriched name data with popularity scores, tiers, metadata

**Table 2**: `usa_names_analysis_summary` (~6 rows)  
- Aggregated statistics by gender and popularity tier

## Technology Stack
- Apache Airflow 2.8.1
- Google Cloud BigQuery
- Python 3.8, pandas 2.0.3
- apache-airflow-providers-google 10.11.0

---

**Author:** Sherif Elashmawy  
**Date:** January 2026  
**Status**: ✅ Successfully executed and verified in BigQuery
