"""
Exercise 07: USA Names Analysis
Author: Sherif Elashmawy
Date: January 2026

This script:
1. Uses BigQuery public data for USA names
2. Finds the most common male and female name for every year
3. Saves results to BigQuery table using action query
4. Data is NOT moved from BigQuery (stays in BigQuery throughout)
"""

from google.cloud import bigquery
import pandas as pd

# Initialize BigQuery client
client = bigquery.Client()

# Public dataset reference
SOURCE_TABLE = "bigquery-public-data.usa_names.usa_1910_current"

# Your project and dataset for results
PROJECT_ID = "data-analytics-project-482302"
DATASET_ID = "usa_names_analysis"
TABLE_ID = "US_common_names"


def create_dataset_if_not_exists():
    """
    Create the dataset if it doesn't already exist.
    """
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    
    try:
        client.get_dataset(dataset_id)
        print(f"✓ Dataset {DATASET_ID} already exists")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"✓ Created dataset {DATASET_ID}")


def explore_usa_names_data():
    """
    Explore the structure and sample data from USA names dataset.
    """
    print("\n" + "="*60)
    print("Exploring USA Names Dataset")
    print("="*60)
    
    # Get schema
    table = client.get_table(SOURCE_TABLE)
    
    print("\nDataset Schema:")
    for field in table.schema:
        print(f"  - {field.name}: {field.field_type}")
    
    # Get sample data
    query = f"""
    SELECT *
    FROM `{SOURCE_TABLE}`
    LIMIT 10
    """
    
    df_sample = client.query(query).to_dataframe()
    
    print("\n\nSample Data (first 10 rows):")
    print(df_sample.to_string(index=False))
    
    # Get total count and year range
    count_query = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT name) as unique_names,
        MIN(year) as earliest_year,
        MAX(year) as latest_year,
        SUM(number) as total_births
    FROM `{SOURCE_TABLE}`
    """
    
    df_stats = client.query(count_query).to_dataframe()
    
    print(f"\n\nDataset Statistics:")
    print(f"  Total Records: {df_stats['total_records'][0]:,}")
    print(f"  Unique Names: {df_stats['unique_names'][0]:,}")
    print(f"  Year Range: {df_stats['earliest_year'][0]} - {df_stats['latest_year'][0]}")
    print(f"  Total Births Recorded: {df_stats['total_births'][0]:,}")
    print()


def create_common_names_table_action_query():
    """
    Create a table with most common male and female names per year.
    Uses an ACTION QUERY - data stays in BigQuery, not moved locally.
    
    This is the key requirement: the query runs entirely in BigQuery,
    and results are saved directly to a BigQuery table.
    """
    print("="*60)
    print("Creating Most Common Names Table (Action Query)")
    print("="*60)
    
    # Create dataset if needed
    create_dataset_if_not_exists()
    
    # Define destination table
    destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    print(f"\nDestination Table: {destination_table}")
    print("Query Type: Action Query (data stays in BigQuery)")
    
    # SQL query to find most common male and female names per year
    query = f"""
    CREATE OR REPLACE TABLE `{destination_table}` AS
    WITH RankedNames AS (
      SELECT 
        year,
        gender,
        name,
        number,
        ROW_NUMBER() OVER (PARTITION BY year, gender ORDER BY number DESC) as rank
      FROM `{SOURCE_TABLE}`
    )
    SELECT 
      year,
      MAX(CASE WHEN gender = 'M' AND rank = 1 THEN name END) as most_common_male_name,
      MAX(CASE WHEN gender = 'M' AND rank = 1 THEN number END) as male_name_count,
      MAX(CASE WHEN gender = 'F' AND rank = 1 THEN name END) as most_common_female_name,
      MAX(CASE WHEN gender = 'F' AND rank = 1 THEN number END) as female_name_count
    FROM RankedNames
    WHERE rank = 1
    GROUP BY year
    ORDER BY year
    """
    
    print("\nExecuting action query...")
    print("(This creates the table directly in BigQuery)")
    
    # Configure job to create table
    job_config = bigquery.QueryJobConfig(
        # No destination needed - it's in the query itself
        use_legacy_sql=False
    )
    
    # Execute the query
    query_job = client.query(query, job_config=job_config)
    
    # Wait for completion
    query_job.result()
    
    print(f"\n✓ Action query completed successfully!")
    print(f"✓ Table created: {destination_table}")
    
    # Get table info
    table = client.get_table(destination_table)
    print(f"✓ Rows: {table.num_rows:,}")
    print(f"✓ Size: {table.num_bytes / 1024:.2f} KB")
    print(f"\nNote: Data was never moved from BigQuery - processed entirely in BigQuery!")
    print()


def verify_results():
    """
    Verify the created table by querying a sample of results.
    """
    print("="*60)
    print("Verifying Results")
    print("="*60)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Query most recent 10 years
    query = f"""
    SELECT *
    FROM `{table_ref}`
    ORDER BY year DESC
    LIMIT 10
    """
    
    df_recent = client.query(query).to_dataframe()
    
    print(f"\nMost Recent 10 Years:")
    print(df_recent.to_string(index=False))
    
    # Query oldest 10 years
    query_old = f"""
    SELECT *
    FROM `{table_ref}`
    ORDER BY year ASC
    LIMIT 10
    """
    
    df_old = client.query(query_old).to_dataframe()
    
    print(f"\n\nOldest 10 Years:")
    print(df_old.to_string(index=False))
    print()


def analyze_name_trends():
    """
    Analyze interesting trends in the most common names.
    """
    print("="*60)
    print("Name Trends Analysis")
    print("="*60)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Most popular male names across all years
    query_male = f"""
    SELECT 
      most_common_male_name as name,
      COUNT(*) as years_as_top_name,
      MIN(year) as first_year,
      MAX(year) as last_year
    FROM `{table_ref}`
    GROUP BY most_common_male_name
    ORDER BY years_as_top_name DESC
    LIMIT 10
    """
    
    df_male = client.query(query_male).to_dataframe()
    
    print("\nTop Male Names by Years as #1:")
    print(df_male.to_string(index=False))
    
    # Most popular female names across all years
    query_female = f"""
    SELECT 
      most_common_female_name as name,
      COUNT(*) as years_as_top_name,
      MIN(year) as first_year,
      MAX(year) as last_year
    FROM `{table_ref}`
    GROUP BY most_common_female_name
    ORDER BY years_as_top_name DESC
    LIMIT 10
    """
    
    df_female = client.query(query_female).to_dataframe()
    
    print("\n\nTop Female Names by Years as #1:")
    print(df_female.to_string(index=False))
    
    # Decades analysis
    query_decades = f"""
    SELECT 
      FLOOR(year / 10) * 10 as decade,
      most_common_male_name,
      most_common_female_name,
      male_name_count,
      female_name_count
    FROM `{table_ref}`
    WHERE MOD(year, 10) = 0  -- Get first year of each decade
    ORDER BY decade DESC
    """
    
    df_decades = client.query(query_decades).to_dataframe()
    
    print("\n\nMost Common Names by Decade:")
    print(df_decades.to_string(index=False))
    print()


def export_sample_to_csv(filename='us_common_names_sample.csv'):
    """
    Export a sample of results to CSV for local viewing.
    
    Args:
        filename: Output CSV filename
    """
    print("="*60)
    print("Exporting Sample to CSV")
    print("="*60)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Get all data (it's not that large)
    query = f"""
    SELECT *
    FROM `{table_ref}`
    ORDER BY year
    """
    
    df = client.query(query).to_dataframe()
    df.to_csv(filename, index=False)
    
    print(f"\n✓ Exported {len(df)} years to: {filename}")
    print()


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 07: USA Names Analysis")
    print("="*60)
    print(f"Project: {PROJECT_ID}")
    print(f"Source: {SOURCE_TABLE}")
    print("="*60)
    
    # Explore the dataset
    explore_usa_names_data()
    
    # Create common names table using action query
    # IMPORTANT: Data stays in BigQuery throughout
    create_common_names_table_action_query()
    
    # Verify the results
    verify_results()
    
    # Analyze name trends
    analyze_name_trends()
    
    # Export sample to CSV
    export_sample_to_csv()
    
    print("\n" + "="*60)
    print("✓ All tasks completed successfully!")
    print("="*60 + "\n")
    
    print("Key Achievement:")
    print("  ✓ Used ACTION QUERY - data never left BigQuery")
    print("  ✓ Processed entirely in BigQuery's servers")
    print("  ✓ Results saved directly to BigQuery table")
    print()
    
    print("Outputs:")
    print(f"  - BigQuery Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"  - CSV Sample: us_common_names_sample.csv")
    print(f"\nView table at:")
    print(f"  https://console.cloud.google.com/bigquery?project={PROJECT_ID}&ws=!1m5!1m4!4m3!1s{PROJECT_ID}!2s{DATASET_ID}!3s{TABLE_ID}")
    print()


if __name__ == "__main__":
    main()
