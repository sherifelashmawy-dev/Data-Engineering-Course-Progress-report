"""
Exercise 11: Data Quality and Profiling Analysis
Author: Sherif Elashmawy
Date: January 2026

This script performs comprehensive data quality analysis on the cars dataset:
- Missing value analysis
- Duplicate detection
- Outlier detection
- Data type validation
- Statistical profiling
- Data quality report generation
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

# Data file
DATA_FILE = 'cars.csv'


def load_data():
    """
    Load the cars dataset.
    
    Returns:
        pd.DataFrame: Loaded data
    """
    print("\n" + "="*60)
    print("Loading Data")
    print("="*60)
    
    df = pd.read_csv(DATA_FILE)
    
    print(f"\n✓ Data loaded successfully")
    print(f"✓ Rows: {len(df):,}")
    print(f"✓ Columns: {len(df.columns)}")
    print()
    
    return df


def basic_overview(df):
    """
    Display basic dataset overview.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Basic Dataset Overview")
    print("="*60)
    
    print(f"\nDataset Shape: {df.shape}")
    print(f"Rows: {df.shape[0]:,}")
    print(f"Columns: {df.shape[1]}")
    
    print(f"\n\nColumn Names and Types:")
    print(df.dtypes.to_string())
    
    print(f"\n\nFirst 10 Rows:")
    print(df.head(10).to_string(index=False))
    
    print(f"\n\nBasic Statistics:")
    print(df.describe().to_string())
    
    print()


def missing_value_analysis(df):
    """
    Analyze missing values in the dataset.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        pd.DataFrame: Missing value summary
    """
    print("="*60)
    print("Missing Value Analysis")
    print("="*60)
    
    # Calculate missing values
    missing_counts = df.isnull().sum()
    missing_percentages = (df.isnull().sum() / len(df)) * 100
    
    missing_df = pd.DataFrame({
        'Column': missing_counts.index,
        'Missing_Count': missing_counts.values,
        'Missing_Percentage': missing_percentages.values
    })
    
    missing_df = missing_df[missing_df['Missing_Count'] > 0].sort_values(
        'Missing_Percentage', ascending=False
    )
    
    if len(missing_df) > 0:
        print(f"\n✓ Found {len(missing_df)} columns with missing values:\n")
        print(missing_df.to_string(index=False))
        
        print(f"\n\nTotal Missing Values: {missing_counts.sum():,}")
        print(f"Percentage of Dataset: {(missing_counts.sum() / (len(df) * len(df.columns))) * 100:.2f}%")
    else:
        print("\n✓ No missing values found!")
    
    print()
    
    return missing_df


def duplicate_detection(df):
    """
    Detect duplicate rows in the dataset.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Duplicate Detection")
    print("="*60)
    
    # Check for exact duplicates
    duplicates = df.duplicated()
    duplicate_count = duplicates.sum()
    
    print(f"\nExact Duplicates: {duplicate_count}")
    
    if duplicate_count > 0:
        print(f"Percentage: {(duplicate_count / len(df)) * 100:.2f}%")
        print(f"\nSample Duplicate Rows:")
        print(df[duplicates].head().to_string(index=False))
    else:
        print("✓ No exact duplicates found!")
    
    # Check for duplicates in specific columns
    key_columns = ['VehicleClass', 'RegDate', 'Brand', 'Year']
    existing_keys = [col for col in key_columns if col in df.columns]
    
    if existing_keys:
        key_duplicates = df.duplicated(subset=existing_keys)
        key_duplicate_count = key_duplicates.sum()
        
        print(f"\n\nDuplicates based on {existing_keys}: {key_duplicate_count}")
        if key_duplicate_count > 0:
            print(f"Percentage: {(key_duplicate_count / len(df)) * 100:.2f}%")
    
    print()


def outlier_detection(df):
    """
    Detect outliers in numeric columns using IQR method.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        dict: Outlier information per column
    """
    print("="*60)
    print("Outlier Detection (IQR Method)")
    print("="*60)
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    outlier_info = {}
    
    for col in numeric_cols:
        if col == 'index':  # Skip index column
            continue
        
        # Calculate IQR
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        # Define outlier bounds
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Find outliers
        outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
        
        outlier_info[col] = {
            'count': len(outliers),
            'percentage': (len(outliers) / len(df.dropna(subset=[col]))) * 100,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound
        }
        
        if len(outliers) > 0:
            print(f"\n{col}:")
            print(f"  Outliers: {len(outliers)} ({outlier_info[col]['percentage']:.2f}%)")
            print(f"  Valid Range: [{lower_bound:.2f}, {upper_bound:.2f}]")
            print(f"  Min Outlier: {outliers.min():.2f}")
            print(f"  Max Outlier: {outliers.max():.2f}")
    
    print()
    
    return outlier_info


def data_type_validation(df):
    """
    Validate and analyze data types.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Data Type Validation")
    print("="*60)
    
    print("\nColumn Data Types:")
    for col in df.columns:
        dtype = df[col].dtype
        unique_count = df[col].nunique()
        print(f"  {col}: {dtype} ({unique_count:,} unique values)")
    
    # Check for potential date columns
    print("\n\nPotential Date Columns:")
    for col in df.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            try:
                pd.to_datetime(df[col])
                print(f"  ✓ {col}: Can be converted to datetime")
            except:
                print(f"  ✗ {col}: Cannot be converted to datetime")
    
    # Check for categorical columns
    print("\n\nCategorical Columns (<=20 unique values):")
    for col in df.columns:
        if df[col].nunique() <= 20 and df[col].dtype == 'object':
            print(f"  {col}: {df[col].nunique()} unique values")
            print(f"    Values: {list(df[col].unique()[:10])}")
    
    print()


def create_missing_value_visualization(df, missing_df):
    """
    Create visualization of missing values.
    
    Args:
        df: DataFrame to analyze
        missing_df: DataFrame with missing value info
    """
    print("="*60)
    print("Creating Missing Value Visualization")
    print("="*60)
    
    if len(missing_df) == 0:
        print("\n✓ No missing values to visualize")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Bar chart of missing values
    ax1.barh(missing_df['Column'], missing_df['Missing_Percentage'], color='#E74C3C')
    ax1.set_xlabel('Missing Percentage (%)', fontweight='bold')
    ax1.set_ylabel('Column', fontweight='bold')
    ax1.set_title('Missing Values by Column', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='x')
    
    # Heatmap of missing values pattern
    ax2.set_title('Missing Values Pattern', fontsize=14, fontweight='bold')
    missing_matrix = df[missing_df['Column']].isnull().astype(int)
    if len(missing_matrix) > 100:
        missing_matrix = missing_matrix.sample(100, random_state=42)
    
    sns.heatmap(missing_matrix.T, cbar=True, cmap='RdYlGn_r', ax=ax2)
    ax2.set_xlabel('Row Sample', fontweight='bold')
    ax2.set_ylabel('Columns with Missing Values', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('missing_values_analysis.png', dpi=300, bbox_inches='tight')
    print("\n✓ Visualization saved to: missing_values_analysis.png")
    plt.close()


def create_distribution_plots(df):
    """
    Create distribution plots for numeric columns.
    
    Args:
        df: DataFrame to analyze
    """
    print("\n" + "="*60)
    print("Creating Distribution Plots")
    print("="*60)
    
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col != 'index']
    
    if len(numeric_cols) == 0:
        print("\n✓ No numeric columns to visualize")
        return
    
    n_cols = min(len(numeric_cols), 3)
    n_rows = (len(numeric_cols) + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 5*n_rows))
    if n_rows == 1 and n_cols == 1:
        axes = np.array([axes])
    axes = axes.flatten()
    
    for idx, col in enumerate(numeric_cols):
        # Remove NaN values
        data = df[col].dropna()
        
        # Create histogram
        axes[idx].hist(data, bins=30, color='#3498DB', alpha=0.7, edgecolor='black')
        axes[idx].set_title(f'Distribution of {col}', fontweight='bold')
        axes[idx].set_xlabel(col)
        axes[idx].set_ylabel('Frequency')
        axes[idx].grid(True, alpha=0.3, axis='y')
        
        # Add mean line
        mean_val = data.mean()
        axes[idx].axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_val:.2f}')
        axes[idx].legend()
    
    # Hide extra subplots
    for idx in range(len(numeric_cols), len(axes)):
        axes[idx].axis('off')
    
    plt.tight_layout()
    plt.savefig('distributions.png', dpi=300, bbox_inches='tight')
    print("\n✓ Distribution plots saved to: distributions.png")
    plt.close()


def create_correlation_heatmap(df):
    """
    Create correlation heatmap for numeric columns.
    
    Args:
        df: DataFrame to analyze
    """
    print("\n" + "="*60)
    print("Creating Correlation Heatmap")
    print("="*60)
    
    numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns if col != 'index']
    
    if len(numeric_cols) < 2:
        print("\n✓ Not enough numeric columns for correlation analysis")
        return
    
    # Calculate correlation matrix
    corr_matrix = df[numeric_cols].corr()
    
    # Create heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, 
                fmt='.2f', square=True, linewidths=1)
    plt.title('Correlation Matrix', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('correlation_heatmap.png', dpi=300, bbox_inches='tight')
    print("\n✓ Correlation heatmap saved to: correlation_heatmap.png")
    plt.close()


def generate_quality_report(df, missing_df, outlier_info):
    """
    Generate a comprehensive data quality report.
    
    Args:
        df: DataFrame analyzed
        missing_df: Missing value summary
        outlier_info: Outlier information
    """
    print("="*60)
    print("Generating Data Quality Report")
    print("="*60)
    
    report_lines = []
    report_lines.append("="*60)
    report_lines.append("DATA QUALITY REPORT")
    report_lines.append("="*60)
    report_lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Dataset: {DATA_FILE}")
    
    # Dataset Overview
    report_lines.append("\n\n" + "="*60)
    report_lines.append("DATASET OVERVIEW")
    report_lines.append("="*60)
    report_lines.append(f"\nTotal Rows: {len(df):,}")
    report_lines.append(f"Total Columns: {len(df.columns)}")
    report_lines.append(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Data Types
    report_lines.append("\n\nData Types:")
    for dtype, count in df.dtypes.value_counts().items():
        report_lines.append(f"  {dtype}: {count} columns")
    
    # Missing Values
    report_lines.append("\n\n" + "="*60)
    report_lines.append("MISSING VALUES")
    report_lines.append("="*60)
    
    if len(missing_df) > 0:
        report_lines.append(f"\nColumns with Missing Values: {len(missing_df)}")
        report_lines.append(f"Total Missing Values: {missing_df['Missing_Count'].sum():,}")
        report_lines.append("\nDetails:")
        for _, row in missing_df.iterrows():
            report_lines.append(f"  {row['Column']}: {row['Missing_Count']:,} ({row['Missing_Percentage']:.2f}%)")
    else:
        report_lines.append("\n✓ No missing values detected")
    
    # Duplicates
    report_lines.append("\n\n" + "="*60)
    report_lines.append("DUPLICATES")
    report_lines.append("="*60)
    
    duplicate_count = df.duplicated().sum()
    report_lines.append(f"\nExact Duplicates: {duplicate_count}")
    if duplicate_count > 0:
        report_lines.append(f"Percentage: {(duplicate_count / len(df)) * 100:.2f}%")
    else:
        report_lines.append("✓ No exact duplicates found")
    
    # Outliers
    report_lines.append("\n\n" + "="*60)
    report_lines.append("OUTLIERS")
    report_lines.append("="*60)
    
    total_outliers = sum(info['count'] for info in outlier_info.values())
    report_lines.append(f"\nTotal Outliers Detected: {total_outliers}")
    
    if total_outliers > 0:
        report_lines.append("\nDetails:")
        for col, info in outlier_info.items():
            if info['count'] > 0:
                report_lines.append(f"  {col}: {info['count']} ({info['percentage']:.2f}%)")
    
    # Quality Score
    report_lines.append("\n\n" + "="*60)
    report_lines.append("DATA QUALITY SCORE")
    report_lines.append("="*60)
    
    # Calculate quality score (0-100)
    completeness = (1 - (missing_df['Missing_Count'].sum() / (len(df) * len(df.columns)))) * 100 if len(missing_df) > 0 else 100
    uniqueness = (1 - (duplicate_count / len(df))) * 100 if len(df) > 0 else 100
    
    quality_score = (completeness * 0.6 + uniqueness * 0.4)
    
    report_lines.append(f"\nCompleteness: {completeness:.2f}%")
    report_lines.append(f"Uniqueness: {uniqueness:.2f}%")
    report_lines.append(f"\nOverall Quality Score: {quality_score:.2f}/100")
    
    if quality_score >= 90:
        report_lines.append("Rating: EXCELLENT")
    elif quality_score >= 75:
        report_lines.append("Rating: GOOD")
    elif quality_score >= 60:
        report_lines.append("Rating: FAIR")
    else:
        report_lines.append("Rating: POOR")
    
    report_lines.append("\n" + "="*60)
    
    # Write to file
    report_text = "\n".join(report_lines)
    
    with open('data_quality_report.txt', 'w') as f:
        f.write(report_text)
    
    print("\n✓ Quality report saved to: data_quality_report.txt")
    print(f"\n{report_text}")
    print()


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 11: Data Quality and Profiling Analysis")
    print("="*60)
    
    # Load data
    df = load_data()
    
    # Basic overview
    basic_overview(df)
    
    # Missing value analysis
    missing_df = missing_value_analysis(df)
    
    # Duplicate detection
    duplicate_detection(df)
    
    # Outlier detection
    outlier_info = outlier_detection(df)
    
    # Data type validation
    data_type_validation(df)
    
    # Create visualizations
    create_missing_value_visualization(df, missing_df)
    create_distribution_plots(df)
    create_correlation_heatmap(df)
    
    # Generate quality report
    generate_quality_report(df, missing_df, outlier_info)
    
    print("\n" + "="*60)
    print("✓ All analysis completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print("  - data_quality_report.txt")
    print("  - missing_values_analysis.png")
    print("  - distributions.png")
    print("  - correlation_heatmap.png")
    print()


if __name__ == "__main__":
    main()
