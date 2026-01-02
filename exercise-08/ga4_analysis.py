"""
Exercise 08: Google Analytics 4 Data Analysis
Author: Sherif Elashmawy
Date: January 2026

This script performs GA4 data analysis:
a) Query GA4 data for date range (2021-01-15 to 2021-01-31)
b) Save results to BigQuery table (data stays in BigQuery)
c) Query the created table and import to pandas
d) Visualize top 20 customers by revenue
"""

from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt

# Initialize BigQuery client
client = bigquery.Client()

# Source table (wildcard table)
SOURCE_TABLE = "bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*"

# Your project and dataset for results
PROJECT_ID = "data-analytics-project-482302"
DATASET_ID = "G4_daily_user"
TABLE_ID = "G4_daily_user_data"


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


def explore_ga4_data():
    """
    Explore the GA4 dataset structure.
    """
    print("\n" + "="*60)
    print("Exploring GA4 Dataset Structure")
    print("="*60)
    
    # Query a single day to see structure
    query = """
    SELECT *
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210115`
    LIMIT 5
    """
    
    print("\nQuerying sample data from 2021-01-15...")
    df_sample = client.query(query).to_dataframe()
    
    print(f"\nColumns in GA4 events table:")
    for col in df_sample.columns:
        print(f"  - {col}")
    
    print(f"\nSample data shape: {df_sample.shape}")
    print()


def query_ga4_user_metrics():
    """
    Task a) Query GA4 data from 2021-01-15 to 2021-01-31.
    Calculate user_pseudo_id, number of events, and total revenue per user.
    
    Task b) Save results directly to BigQuery table (action query).
    """
    print("="*60)
    print("Task a & b: Query GA4 Data and Save to BigQuery")
    print("="*60)
    
    # Create dataset if needed
    create_dataset_if_not_exists()
    
    destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    print(f"\nDate Range: 2021-01-15 to 2021-01-31")
    print(f"Destination: {destination_table}")
    print("Method: Action Query (data stays in BigQuery)")
    
    # Query with wildcard table and _TABLE_SUFFIX
    query = f"""
    CREATE OR REPLACE TABLE `{destination_table}` AS
    SELECT 
        user_pseudo_id,
        COUNT(*) as event_count,
        SUM((
            SELECT COALESCE(SUM(CAST(param.value.int_value AS FLOAT64)), 0)
            FROM UNNEST(event_params) AS param
            WHERE param.key = 'value'
        )) / 1000000 as total_revenue
    FROM `{SOURCE_TABLE}`
    WHERE _TABLE_SUFFIX BETWEEN '20210115' AND '20210131'
    GROUP BY user_pseudo_id
    HAVING total_revenue > 0
    ORDER BY total_revenue DESC
    """
    
    print("\nExecuting query...")
    print("(Processing data in BigQuery, saving to table)")
    
    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    query_job = client.query(query, job_config=job_config)
    query_job.result()
    
    print(f"\n✓ Query completed successfully!")
    
    # Get table info
    table = client.get_table(destination_table)
    print(f"✓ Table created: {destination_table}")
    print(f"✓ Rows: {table.num_rows:,}")
    print(f"✓ Size: {table.num_bytes / 1024:.2f} KB")
    print()


def import_to_pandas():
    """
    Task c) Query the created table and import to local pandas DataFrame.
    """
    print("="*60)
    print("Task c: Import Data to Pandas DataFrame")
    print("="*60)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    query = f"""
    SELECT *
    FROM `{table_ref}`
    ORDER BY total_revenue DESC
    """
    
    print(f"\nQuerying table: {table_ref}")
    print("Importing to local pandas DataFrame...")
    
    df = client.query(query).to_dataframe()
    
    print(f"\n✓ Data imported successfully!")
    print(f"✓ DataFrame shape: {df.shape}")
    print(f"✓ Total users: {len(df):,}")
    
    print(f"\nDataFrame Info:")
    print(df.info())
    
    print(f"\nTop 10 Users by Revenue:")
    print(df.head(10).to_string(index=False))
    
    print(f"\nBasic Statistics:")
    print(df.describe())
    print()
    
    return df


def visualize_top_customers(df, output_file='top_20_customers_revenue.png'):
    """
    Task d) Visualize top 20 customers by revenue.
    
    Args:
        df: DataFrame with user data
        output_file: Output filename for plot
    """
    print("="*60)
    print("Task d: Visualize Top 20 Customers by Revenue")
    print("="*60)
    
    # Get top 20 customers
    df_top20 = df.head(20).copy()
    
    # Create bar chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Top 20 Revenue
    ax1.barh(range(20), df_top20['total_revenue'], color='#2E86AB')
    ax1.set_yticks(range(20))
    ax1.set_yticklabels([f"User {i+1}" for i in range(20)])
    ax1.set_xlabel('Total Revenue ($)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('User Rank', fontsize=12, fontweight='bold')
    ax1.set_title('Top 20 Customers by Revenue', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='x')
    ax1.invert_yaxis()
    
    # Add value labels
    for i, v in enumerate(df_top20['total_revenue']):
        ax1.text(v, i, f' ${v:,.2f}', va='center', fontsize=9)
    
    # Plot 2: Top 20 Event Counts
    ax2.barh(range(20), df_top20['event_count'], color='#A23B72')
    ax2.set_yticks(range(20))
    ax2.set_yticklabels([f"User {i+1}" for i in range(20)])
    ax2.set_xlabel('Number of Events', fontsize=12, fontweight='bold')
    ax2.set_ylabel('User Rank', fontsize=12, fontweight='bold')
    ax2.set_title('Top 20 Customers by Activity', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='x')
    ax2.invert_yaxis()
    
    # Add value labels
    for i, v in enumerate(df_top20['event_count']):
        ax2.text(v, i, f' {int(v):,}', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Visualization saved to: {output_file}")
    plt.close()
    
    # Additional visualizations
    create_additional_charts(df)


def create_additional_charts(df):
    """
    Create additional analysis charts.
    
    Args:
        df: DataFrame with user data
    """
    print("\nCreating additional analysis charts...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('GA4 User Analysis Dashboard', fontsize=16, fontweight='bold')
    
    # Plot 1: Revenue Distribution
    ax1 = axes[0, 0]
    ax1.hist(df['total_revenue'], bins=50, color='#2E86AB', alpha=0.7, edgecolor='black')
    ax1.set_xlabel('Total Revenue ($)')
    ax1.set_ylabel('Number of Users')
    ax1.set_title('Revenue Distribution')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Plot 2: Event Count Distribution
    ax2 = axes[0, 1]
    ax2.hist(df['event_count'], bins=50, color='#A23B72', alpha=0.7, edgecolor='black')
    ax2.set_xlabel('Event Count')
    ax2.set_ylabel('Number of Users')
    ax2.set_title('Event Count Distribution')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Plot 3: Revenue vs Events Scatter
    ax3 = axes[1, 0]
    ax3.scatter(df['event_count'], df['total_revenue'], alpha=0.5, color='#F18F01', s=20)
    ax3.set_xlabel('Number of Events')
    ax3.set_ylabel('Total Revenue ($)')
    ax3.set_title('Revenue vs. Activity Level')
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Summary Statistics
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    stats_text = f"""
    Summary Statistics
    
    Total Users: {len(df):,}
    
    Revenue:
      Total: ${df['total_revenue'].sum():,.2f}
      Average: ${df['total_revenue'].mean():,.2f}
      Median: ${df['total_revenue'].median():,.2f}
      Max: ${df['total_revenue'].max():,.2f}
    
    Events:
      Total: {df['event_count'].sum():,}
      Average: {df['event_count'].mean():,.0f}
      Median: {df['event_count'].median():,.0f}
      Max: {int(df['event_count'].max()):,}
    """
    
    ax4.text(0.1, 0.5, stats_text, fontsize=12, verticalalignment='center',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig('ga4_analysis_dashboard.png', dpi=300, bbox_inches='tight')
    print("✓ Dashboard saved to: ga4_analysis_dashboard.png")
    plt.close()


def export_to_csv(df, filename='ga4_user_data.csv'):
    """
    Export data to CSV.
    
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
    print("Exercise 08: Google Analytics 4 Data Analysis")
    print("="*60)
    print(f"Project: {PROJECT_ID}")
    print(f"Source: {SOURCE_TABLE}")
    print("="*60)
    
    # Explore GA4 data structure
    explore_ga4_data()
    
    # Task a & b: Query and save to BigQuery
    query_ga4_user_metrics()
    
    # Task c: Import to pandas
    df = import_to_pandas()
    
    # Task d: Visualize top 20 customers
    visualize_top_customers(df)
    
    # Export to CSV
    export_to_csv(df)
    
    print("\n" + "="*60)
    print("✓ All tasks completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print(f"  - BigQuery Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"  - Top 20 Chart: top_20_customers_revenue.png")
    print(f"  - Dashboard: ga4_analysis_dashboard.png")
    print(f"  - CSV Export: ga4_user_data.csv")
    print(f"\nView table at:")
    print(f"  https://console.cloud.google.com/bigquery?project={PROJECT_ID}&ws=!1m5!1m4!4m3!1s{PROJECT_ID}!2s{DATASET_ID}!3s{TABLE_ID}")
    print()


if __name__ == "__main__":
    main()
