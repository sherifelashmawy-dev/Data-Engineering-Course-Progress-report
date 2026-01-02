"""
Exercise 01: Global Temperature Data Analysis
Author: Sherif Elashmawy
Date: January 2026

This script analyzes global temperature data from GitHub, performing the following tasks:
a) Read data and save to CSV and SQLite database
b) Convert time information to datetime and Unix epoch
c) Reshape data with sources as column headers
d) Compare observations from multiple sources
e) Visualize temperature trends
f) Estimate future temperatures using interpolation
"""

import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
from scipy import interpolate
from datetime import datetime

# Global temperature data URL from GitHub
DATA_URL = "https://raw.githubusercontent.com/datasets/global-temp/master/data/annual.csv"


def load_data():
    """
    Load global temperature data from GitHub.
    
    Returns:
        pd.DataFrame: Raw temperature data
    """
    print("Loading data from GitHub...")
    df = pd.read_csv(DATA_URL)
    print(f"✓ Data loaded successfully: {len(df)} rows")
    return df


def save_to_csv(df, filename='global_temperature.csv'):
    """
    Save DataFrame to local CSV file.
    
    Args:
        df: DataFrame to save
        filename: Output CSV filename
    """
    df.to_csv(filename, index=False)
    print(f"✓ Data saved to {filename}")


def save_to_sqlite(df, db_name='temperature_data.db', table_name='global_temp'):
    """
    Save DataFrame to SQLite database.
    
    Args:
        df: DataFrame to save
        db_name: SQLite database filename
        table_name: Table name in database
    """
    # Connect to SQLite database (creates if doesn't exist)
    conn = sqlite3.connect(db_name)
    
    # Write DataFrame to SQL table
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"✓ Data saved to SQLite database: {db_name}, table: {table_name}")


def convert_time_formats(df):
    """
    Convert time information to datetime format and Unix epoch.
    
    Args:
        df: DataFrame with 'Year' column
        
    Returns:
        pd.DataFrame: DataFrame with added datetime and epoch columns
    """
    # Convert Year to datetime format (assuming January 1st of each year)
    df['DateTime'] = pd.to_datetime(df['Year'], format='%Y')
    
    # Convert to Unix epoch (seconds since 1970-01-01)
    df['Unix_Epoch'] = df['DateTime'].astype(np.int64) // 10**9
    
    print("✓ Time formats converted: added DateTime and Unix_Epoch columns")
    return df


def reshape_data_by_source(df):
    """
    Reshape data so that each data source becomes a column header.
    Original format: Year | Source | Mean
    New format: Year | GCAG | GISTEMP
    
    Args:
        df: DataFrame with columns [Year, Source, Mean]
        
    Returns:
        pd.DataFrame: Reshaped DataFrame with sources as columns
    """
    # Pivot the data: Year as index, Source values become columns
    df_pivoted = df.pivot(index='Year', columns='Source', values='Mean')
    
    # Reset index to make Year a regular column
    df_pivoted = df_pivoted.reset_index()
    
    print("✓ Data reshaped with sources as column headers")
    print(f"  Columns: {list(df_pivoted.columns)}")
    
    return df_pivoted


def compare_sources(df_pivoted):
    """
    Compare temperature observations from two sources when both are available.
    
    Args:
        df_pivoted: DataFrame with sources as columns
        
    Returns:
        pd.DataFrame: Comparison statistics
    """
    # Get the source column names (excluding 'Year')
    sources = [col for col in df_pivoted.columns if col != 'Year']
    
    if len(sources) < 2:
        print("⚠ Not enough sources for comparison")
        return None
    
    source1, source2 = sources[0], sources[1]
    
    # Filter rows where both sources have data
    both_available = df_pivoted[[source1, source2]].notna().all(axis=1)
    df_both = df_pivoted[both_available]
    
    # Calculate difference between sources
    df_both['Difference'] = df_both[source1] - df_both[source2]
    
    # Calculate statistics
    stats = {
        'Records with both sources': len(df_both),
        'Mean difference': df_both['Difference'].mean(),
        'Std deviation of difference': df_both['Difference'].std(),
        'Max difference': df_both['Difference'].max(),
        'Min difference': df_both['Difference'].min(),
        'Correlation': df_both[source1].corr(df_both[source2])
    }
    
    print(f"\n=== Comparison: {source1} vs {source2} ===")
    for key, value in stats.items():
        print(f"{key}: {value:.4f}")
    
    return df_both


def visualize_trends(df_pivoted, save_path='temperature_trends.png'):
    """
    Visualize global temperature trends separately for each source.
    
    Args:
        df_pivoted: DataFrame with sources as columns
        save_path: Path to save the plot
    """
    sources = [col for col in df_pivoted.columns if col != 'Year']
    
    plt.figure(figsize=(12, 6))
    
    for source in sources:
        # Filter out NaN values
        mask = df_pivoted[source].notna()
        plt.plot(df_pivoted[mask]['Year'], 
                df_pivoted[mask][source], 
                marker='o', 
                label=source, 
                linewidth=2,
                markersize=4)
    
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Temperature Anomaly (°C)', fontsize=12)
    plt.title('Global Temperature Trends by Data Source', fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"✓ Temperature trends plot saved to {save_path}")
    plt.close()


def estimate_future_temps(df_pivoted, years_ahead=10, save_path='future_estimation.png'):
    """
    Estimate future temperatures using interpolation methods.
    
    Args:
        df_pivoted: DataFrame with sources as columns
        years_ahead: Number of years to predict into the future
        save_path: Path to save the plot
    """
    sources = [col for col in df_pivoted.columns if col != 'Year']
    
    # Use the first available source for prediction
    source = sources[0]
    
    # Get data without NaN values
    mask = df_pivoted[source].notna()
    years = df_pivoted[mask]['Year'].values
    temps = df_pivoted[mask][source].values
    
    # Create future years
    last_year = years[-1]
    future_years = np.arange(last_year + 1, last_year + years_ahead + 1)
    all_years = np.concatenate([years, future_years])
    
    # Apply different interpolation methods
    methods = {
        'Linear': 'linear',
        'Quadratic': 'quadratic',
        'Cubic': 'cubic'
    }
    
    plt.figure(figsize=(14, 8))
    
    # Plot historical data
    plt.plot(years, temps, 'ko-', label='Historical Data', linewidth=2, markersize=6)
    
    # Plot predictions using different methods
    colors = ['blue', 'red', 'green']
    for (method_name, method), color in zip(methods.items(), colors):
        # Create interpolation function
        f = interpolate.interp1d(years, temps, kind=method, fill_value='extrapolate')
        
        # Predict future values
        future_temps = f(future_years)
        
        # Plot predictions
        plt.plot(future_years, future_temps, 
                marker='s', 
                color=color, 
                linestyle='--', 
                label=f'{method_name} Interpolation',
                linewidth=2,
                markersize=6)
    
    plt.axvline(x=last_year, color='gray', linestyle=':', linewidth=2, label='Present')
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Temperature Anomaly (°C)', fontsize=12)
    plt.title(f'Global Temperature: Historical Data and {years_ahead}-Year Predictions', 
             fontsize=14, fontweight='bold')
    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"✓ Future temperature estimation plot saved to {save_path}")
    plt.close()
    
    # Print predictions
    print(f"\n=== Temperature Predictions using {source} data ===")
    for year, method_name in zip(future_years[:5], methods.keys()):
        f = interpolate.interp1d(years, temps, kind=methods[method_name], fill_value='extrapolate')
        pred = f(year)
        print(f"Year {year} ({method_name}): {pred:.3f}°C")


def main():
    """
    Main execution function that runs all analysis steps.
    """
    print("\n" + "="*60)
    print("Exercise 01: Global Temperature Data Analysis")
    print("="*60 + "\n")
    
    # Step a) Load and save data
    df = load_data()
    save_to_csv(df)
    save_to_sqlite(df)
    
    # Step b) Convert time formats
    df = convert_time_formats(df)
    
    # Step c) Reshape data by source
    df_pivoted = reshape_data_by_source(df)
    
    # Step d) Compare sources
    df_comparison = compare_sources(df_pivoted)
    
    # Step e) Visualize trends
    visualize_trends(df_pivoted)
    
    # Step f) Estimate future temperatures
    estimate_future_temps(df_pivoted, years_ahead=10)
    
    print("\n" + "="*60)
    print("✓ All tasks completed successfully!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
