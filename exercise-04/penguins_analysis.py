"""
Exercise 04: BigQuery Penguins Analysis
Author: Sherif Elashmawy
Date: January 2026

This script analyzes penguin data from BigQuery public datasets:
- Calculates average body mass, culmen length, and culmen depth
- Groups by island and sex
- Saves results to BigQuery table
"""

from google.cloud import bigquery
import pandas as pd
import os

# Initialize BigQuery client
# Make sure your Google Cloud credentials are set up
client = bigquery.Client()

# Public dataset reference
SOURCE_TABLE = "bigquery-public-data.ml_datasets.penguins"

# Your project and dataset for results
PROJECT_ID = "data-analytics-project-482302"
DATASET_ID = "penguins_analysis"
TABLE_ID = "penguin_stats_by_island_sex"


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


def explore_penguins_data():
    """
    Explore the structure and sample data from the penguins dataset.
    """
    print("\n" + "="*60)
    print("Exploring Penguins Dataset")
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
    LIMIT 5
    """
    
    df_sample = client.query(query).to_dataframe()
    
    print("\n\nSample Data (first 5 rows):")
    print(df_sample.to_string(index=False))
    
    # Get total count
    count_query = f"""
    SELECT COUNT(*) as total_penguins
    FROM `{SOURCE_TABLE}`
    """
    
    df_count = client.query(count_query).to_dataframe()
    print(f"\n\nTotal Penguins in Dataset: {df_count['total_penguins'][0]}")
    print()


def query_penguin_statistics():
    """
    Query average body mass, culmen length, and culmen depth
    grouped by island and sex.
    
    Returns:
        pd.DataFrame: Query results
    """
    print("="*60)
    print("Calculating Penguin Statistics by Island and Sex")
    print("="*60)
    
    query = f"""
    SELECT 
        island,
        sex,
        COUNT(*) as penguin_count,
        ROUND(AVG(body_mass_g), 2) as avg_body_mass,
        ROUND(AVG(culmen_length_mm), 2) as avg_culmen_length,
        ROUND(AVG(culmen_depth_mm), 2) as avg_culmen_depth,
        ROUND(MIN(body_mass_g), 2) as min_body_mass,
        ROUND(MAX(body_mass_g), 2) as max_body_mass,
        ROUND(STDDEV(body_mass_g), 2) as stddev_body_mass
    FROM 
        `{SOURCE_TABLE}`
    WHERE 
        sex IS NOT NULL
    GROUP BY 
        island, sex
    ORDER BY 
        island, sex
    """
    
    print("\nExecuting query...")
    df = client.query(query).to_dataframe()
    
    print(f"✓ Query completed successfully")
    print(f"✓ Retrieved {len(df)} groups\n")
    
    print("Results:")
    print(df.to_string(index=False))
    print()
    
    return df


def save_results_to_bigquery(df):
    """
    Save query results to a BigQuery table.
    
    Args:
        df: DataFrame to save
    """
    print("="*60)
    print("Saving Results to BigQuery Table")
    print("="*60)
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists()
    
    # Define destination table
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Configure the query job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite if exists
    )
    
    # Load DataFrame to BigQuery
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    
    # Wait for the job to complete
    job.result()
    
    print(f"\n✓ Results saved to table: {table_ref}")
    print(f"✓ Table contains {len(df)} rows")
    
    # Get table info
    table = client.get_table(table_ref)
    print(f"✓ Table size: {table.num_bytes / 1024:.2f} KB")
    print()


def create_summary_analysis(df):
    """
    Create additional summary analysis.
    
    Args:
        df: DataFrame with penguin statistics
    """
    print("="*60)
    print("Summary Analysis")
    print("="*60)
    
    print("\n1. Average Body Mass by Island (all sexes combined):")
    island_avg = df.groupby('island')['avg_body_mass'].mean().sort_values(ascending=False)
    for island, avg_mass in island_avg.items():
        print(f"   {island}: {avg_mass:.2f} g")
    
    print("\n2. Sexual Dimorphism (difference between male and female averages):")
    for island in df['island'].unique():
        island_data = df[df['island'] == island]
        if len(island_data) == 2:  # Has both male and female
            male_mass = island_data[island_data['sex'] == 'MALE']['avg_body_mass'].values[0]
            female_mass = island_data[island_data['sex'] == 'FEMALE']['avg_body_mass'].values[0]
            diff = male_mass - female_mass
            pct_diff = (diff / female_mass) * 100
            print(f"   {island}: {diff:.2f} g ({pct_diff:.1f}% heavier males)")
    
    print("\n3. Penguin Distribution:")
    total_penguins = df['penguin_count'].sum()
    for island in df['island'].unique():
        island_count = df[df['island'] == island]['penguin_count'].sum()
        pct = (island_count / total_penguins) * 100
        print(f"   {island}: {island_count} penguins ({pct:.1f}%)")
    
    print()


def export_to_csv(df, filename='penguin_statistics.csv'):
    """
    Export results to CSV file.
    
    Args:
        df: DataFrame to export
        filename: Output filename
    """
    df.to_csv(filename, index=False)
    print(f"✓ Results exported to: {filename}")


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 04: BigQuery Penguins Analysis")
    print("="*60)
    print(f"Project: {PROJECT_ID}")
    print(f"Source: {SOURCE_TABLE}")
    print("="*60 + "\n")
    
    # Explore the dataset
    explore_penguins_data()
    
    # Query penguin statistics
    df_stats = query_penguin_statistics()
    
    # Save to BigQuery table
    save_results_to_bigquery(df_stats)
    
    # Create summary analysis
    create_summary_analysis(df_stats)
    
    # Export to CSV
    export_to_csv(df_stats)
    
    print("\n" + "="*60)
    print("✓ All tasks completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print(f"  - BigQuery Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"  - CSV File: penguin_statistics.csv")
    print(f"\nView table at:")
    print(f"  https://console.cloud.google.com/bigquery?project={PROJECT_ID}&ws=!1m5!1m4!4m3!1s{PROJECT_ID}!2s{DATASET_ID}!3s{TABLE_ID}")
    print()


if __name__ == "__main__":
    main()
