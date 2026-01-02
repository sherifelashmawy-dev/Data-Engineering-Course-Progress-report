"""
Exercise 06: World Bank Population Data Analysis
Author: Sherif Elashmawy
Date: January 2026

This script:
1. Queries World Bank population data from BigQuery public dataset
2. Downloads entire dataset to local computer using BigQuery API
3. Transforms data to have year as rows (melting the wide format)
4. Creates table for Nordic countries population
5. Uploads transformed data back to BigQuery using API
"""

from google.cloud import bigquery
import pandas as pd
import os

# Initialize BigQuery client
client = bigquery.Client()

# Public dataset reference
SOURCE_TABLE = "bigquery-public-data.world_bank_global_population.population_by_country"

# Your project and dataset for results
PROJECT_ID = "data-analytics-project-482302"
DATASET_ID = "world_bank_analysis"
TABLE_ID = "nordic_population"

# Nordic countries
NORDIC_COUNTRIES = ['Finland', 'Sweden', 'Norway', 'Denmark', 'Iceland']


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


def query_entire_dataset():
    """
    Query the entire World Bank population dataset to local computer.
    
    Returns:
        pd.DataFrame: Complete population data (in wide format)
    """
    print("\n" + "="*60)
    print("Querying Entire World Bank Population Dataset")
    print("="*60)
    
    query = f"""
    SELECT *
    FROM `{SOURCE_TABLE}`
    ORDER BY country
    """
    
    print("\nExecuting query... (downloading entire dataset)")
    df = client.query(query).to_dataframe()
    
    print(f"✓ Query completed successfully")
    print(f"✓ Downloaded {len(df):,} countries")
    print(f"✓ Year columns: {len([c for c in df.columns if c.startswith('year_')])}")
    print()
    
    return df


def transform_to_year_rows(df):
    """
    Transform data from wide format (year_XXXX columns) to long format.
    Original: country | country_code | year_1960 | year_1961 | ...
    New: country | country_code | year | population
    
    Args:
        df: DataFrame in wide format
        
    Returns:
        pd.DataFrame: Data in long format
    """
    print("="*60)
    print("Transforming Data to Long Format")
    print("="*60)
    
    print("\nOriginal format (wide - years as columns):")
    print(f"  Shape: {df.shape}")
    print(f"  Sample columns: {list(df.columns[:5])}")
    
    # Get year columns
    year_columns = [col for col in df.columns if col.startswith('year_')]
    
    # Melt the DataFrame to convert year columns to rows
    df_long = df.melt(
        id_vars=['country', 'country_code'],
        value_vars=year_columns,
        var_name='year_col',
        value_name='population'
    )
    
    # Extract year from column name (year_1960 -> 1960)
    df_long['year'] = df_long['year_col'].str.replace('year_', '').astype(int)
    
    # Drop the year_col column and remove rows with null population
    df_long = df_long[['country', 'country_code', 'year', 'population']].dropna(subset=['population'])
    
    # Sort by country and year
    df_long = df_long.sort_values(['country', 'year'])
    
    print(f"\nTransformed format (long - years as rows):")
    print(f"  Shape: {df_long.shape}")
    print(f"  Columns: {list(df_long.columns)}")
    print(f"\nSample data:")
    print(df_long.head(10).to_string(index=False))
    print()
    
    return df_long


def create_nordic_wide_format(df_long):
    """
    Create Nordic countries table in wide format (year | Finland | Sweden | ...).
    
    Args:
        df_long: DataFrame in long format
        
    Returns:
        pd.DataFrame: Nordic countries in wide format
    """
    print("="*60)
    print("Creating Nordic Countries Table (Wide Format)")
    print("="*60)
    
    # Filter for Nordic countries
    df_nordic = df_long[df_long['country'].isin(NORDIC_COUNTRIES)].copy()
    
    print(f"\n✓ Found Nordic countries:")
    for country in NORDIC_COUNTRIES:
        if country in df_nordic['country'].values:
            print(f"  - {country}")
    
    # Pivot to wide format: year as rows, countries as columns
    df_nordic_wide = df_nordic.pivot(
        index='year',
        columns='country',
        values='population'
    )
    
    # Reset index to make year a regular column
    df_nordic_wide = df_nordic_wide.reset_index()
    
    # Reorder columns: year, then Nordic countries in specific order
    available_nordics = [c for c in NORDIC_COUNTRIES if c in df_nordic_wide.columns]
    column_order = ['year'] + available_nordics
    df_nordic_wide = df_nordic_wide[column_order]
    
    # Sort by year
    df_nordic_wide = df_nordic_wide.sort_values('year')
    
    print(f"\nNordic Table Structure:")
    print(f"  Columns: {list(df_nordic_wide.columns)}")
    print(f"  Years: {df_nordic_wide['year'].min()} - {df_nordic_wide['year'].max()}")
    print(f"  Rows: {len(df_nordic_wide)}")
    
    print(f"\nFirst 10 years:")
    print(df_nordic_wide.head(10).to_string(index=False))
    
    print(f"\nLast 10 years:")
    print(df_nordic_wide.tail(10).to_string(index=False))
    
    print()
    
    return df_nordic_wide


def create_summary_statistics(df_nordic):
    """
    Create summary statistics for Nordic countries.
    
    Args:
        df_nordic: Nordic countries population DataFrame
    """
    print("="*60)
    print("Nordic Population Statistics")
    print("="*60)
    
    # Get country columns (exclude year)
    countries = [col for col in df_nordic.columns if col != 'year']
    
    print(f"\nPopulation Summary (in millions):")
    print("-" * 60)
    
    for country in countries:
        # Get latest and earliest data
        df_clean = df_nordic[['year', country]].dropna()
        
        if len(df_clean) > 0:
            latest_year = df_clean['year'].max()
            latest_pop = df_clean[df_clean['year'] == latest_year][country].values[0]
            earliest_year = df_clean['year'].min()
            earliest_pop = df_clean[df_clean['year'] == earliest_year][country].values[0]
            
            growth = latest_pop - earliest_pop
            growth_pct = (growth / earliest_pop) * 100
            
            print(f"\n{country}:")
            print(f"  {int(earliest_year)}: {earliest_pop / 1_000_000:.2f}M")
            print(f"  {int(latest_year)}: {latest_pop / 1_000_000:.2f}M")
            print(f"  Growth: {growth / 1_000_000:+.2f}M ({growth_pct:+.1f}%)")
    
    # Total Nordic population
    print(f"\nTotal Nordic Population ({int(df_nordic['year'].max())}):")
    df_latest = df_nordic[df_nordic['year'] == df_nordic['year'].max()]
    total_pop = df_latest[countries].sum(axis=1).values[0]
    print(f"  {total_pop / 1_000_000:.2f} million people")
    
    print()


def upload_to_bigquery(df, table_name=TABLE_ID):
    """
    Upload DataFrame to BigQuery using API.
    
    Args:
        df: DataFrame to upload
        table_name: Name of the destination table
    """
    print("="*60)
    print("Uploading Nordic Data to BigQuery")
    print("="*60)
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists()
    
    # Define destination table
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configure the query job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite if exists
    )
    
    print(f"\nUploading data to: {table_ref}")
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {len(df.columns)}")
    
    # Load DataFrame to BigQuery
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    
    # Wait for the job to complete
    job.result()
    
    print(f"\n✓ Upload completed successfully!")
    
    # Get table info
    table = client.get_table(table_ref)
    print(f"✓ Table: {table_ref}")
    print(f"✓ Rows: {table.num_rows:,}")
    print(f"✓ Size: {table.num_bytes / 1024:.2f} KB")
    print()


def verify_upload():
    """
    Verify the uploaded data by querying it back.
    """
    print("="*60)
    print("Verifying Uploaded Data")
    print("="*60)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Query the uploaded table
    query = f"""
    SELECT *
    FROM `{table_ref}`
    ORDER BY year DESC
    LIMIT 10
    """
    
    df_verify = client.query(query).to_dataframe()
    
    print(f"\nMost Recent 10 Years from BigQuery:")
    print(df_verify.to_string(index=False))
    print()


def export_to_csv(df, filename='nordic_population.csv'):
    """
    Export Nordic data to CSV file.
    
    Args:
        df: DataFrame to export
        filename: Output filename
    """
    df.to_csv(filename, index=False)
    print(f"✓ Data exported to: {filename}")


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 06: World Bank Population Data Analysis")
    print("="*60)
    print(f"Project: {PROJECT_ID}")
    print(f"Source: {SOURCE_TABLE}")
    print("="*60)
    
    # Query entire dataset (already in wide format with year columns)
    df_all = query_entire_dataset()
    
    # Transform to long format (year as rows)
    df_long = transform_to_year_rows(df_all)
    
    # Create Nordic countries table in wide format
    df_nordic = create_nordic_wide_format(df_long)
    
    # Create summary statistics
    create_summary_statistics(df_nordic)
    
    # Upload to BigQuery
    upload_to_bigquery(df_nordic)
    
    # Verify the upload
    verify_upload()
    
    # Export to CSV
    export_to_csv(df_nordic)
    
    print("\n" + "="*60)
    print("✓ All tasks completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print(f"  - BigQuery Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"  - CSV File: nordic_population.csv")
    print(f"\nView table at:")
    print(f"  https://console.cloud.google.com/bigquery?project={PROJECT_ID}&ws=!1m5!1m4!4m3!1s{PROJECT_ID}!2s{DATASET_ID}!3s{TABLE_ID}")
    print()


if __name__ == "__main__":
    main()
