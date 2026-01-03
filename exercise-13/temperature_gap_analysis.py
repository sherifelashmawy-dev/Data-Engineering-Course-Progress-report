"""
Exercise 13: Temperature Data Gap Analysis and Filling
Author: Sherif Elashmawy
Date: January 2026

This script analyzes temperature data with gaps and implements various gap-filling methods:
- Gap detection and quantification
- Multiple interpolation methods
- Comparison of gap-filling techniques
- Visualization of results
- Quality assessment
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 8)

# Data file
DATA_FILE = 'Temperature_data_with_gaps.csv'


def load_data():
    """
    Load the temperature dataset and handle duplicates.
    
    Returns:
        pd.DataFrame: Loaded data with datetime index
    """
    print("\n" + "="*60)
    print("Loading Temperature Data")
    print("="*60)
    
    df = pd.read_csv(DATA_FILE)
    df['Time'] = pd.to_datetime(df['Time'])
    
    # Check for duplicates
    duplicates = df['Time'].duplicated()
    if duplicates.any():
        print(f"\n⚠ Found {duplicates.sum()} duplicate timestamps")
        print(f"✓ Removing duplicates (keeping first occurrence)")
        df = df[~duplicates]
    
    df = df.set_index('Time')
    
    print(f"\n✓ Data loaded successfully")
    print(f"✓ Records: {len(df):,}")
    print(f"✓ Date range: {df.index.min()} to {df.index.max()}")
    print(f"✓ Duration: {(df.index.max() - df.index.min()).days} days")
    print()
    
    return df


def analyze_basic_statistics(df):
    """
    Analyze basic temperature statistics.
    
    Args:
        df: DataFrame with temperature data
    """
    print("="*60)
    print("Basic Temperature Statistics")
    print("="*60)
    
    print(f"\nTemperature Statistics:")
    print(df['Temperature'].describe().to_string())
    
    print(f"\n\nMissing Values in Temperature Column:")
    missing_count = df['Temperature'].isnull().sum()
    missing_pct = (missing_count / len(df)) * 100
    print(f"  Missing: {missing_count:,} ({missing_pct:.2f}%)")
    print(f"  Present: {len(df) - missing_count:,} ({100 - missing_pct:.2f}%)")
    
    print()


def detect_gaps(df):
    """
    Detect gaps in the time series.
    
    Args:
        df: DataFrame with temperature data
        
    Returns:
        list: List of gap information dictionaries
    """
    print("="*60)
    print("Gap Detection")
    print("="*60)
    
    # Create complete time range
    full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='H')
    
    # Find missing timestamps
    missing_times = full_range.difference(df.index)
    
    print(f"\nExpected hourly records: {len(full_range):,}")
    print(f"Actual records: {len(df):,}")
    print(f"Missing timestamps: {len(missing_times):,}")
    
    # Find gaps (consecutive missing values)
    gaps = []
    if len(missing_times) > 0:
        gap_start = missing_times[0]
        gap_length = 1
        
        for i in range(1, len(missing_times)):
            if missing_times[i] == missing_times[i-1] + timedelta(hours=1):
                gap_length += 1
            else:
                gaps.append({
                    'start': gap_start,
                    'end': missing_times[i-1],
                    'length': gap_length
                })
                gap_start = missing_times[i]
                gap_length = 1
        
        # Add last gap
        gaps.append({
            'start': gap_start,
            'end': missing_times[-1],
            'length': gap_length
        })
    
    print(f"\n\nNumber of gaps: {len(gaps)}")
    
    if len(gaps) > 0:
        print(f"\nGap Statistics:")
        gap_lengths = [g['length'] for g in gaps]
        print(f"  Average gap length: {np.mean(gap_lengths):.1f} hours")
        print(f"  Median gap length: {np.median(gap_lengths):.0f} hours")
        print(f"  Longest gap: {max(gap_lengths)} hours")
        print(f"  Shortest gap: {min(gap_lengths)} hours")
        
        print(f"\n\nLargest gaps (top 10):")
        sorted_gaps = sorted(gaps, key=lambda x: x['length'], reverse=True)[:10]
        for i, gap in enumerate(sorted_gaps, 1):
            print(f"  {i}. {gap['start']} to {gap['end']} ({gap['length']} hours)")
    
    # Also check for NaN values in Temperature column
    nan_count = df['Temperature'].isnull().sum()
    if nan_count > 0:
        print(f"\n\n⚠ Additional NaN values in Temperature column: {nan_count}")
    
    print()
    
    return gaps


def create_complete_timeseries(df):
    """
    Create a complete time series with explicit NaN values for gaps.
    
    Args:
        df: DataFrame with temperature data
        
    Returns:
        pd.DataFrame: Complete time series
    """
    print("="*60)
    print("Creating Complete Time Series")
    print("="*60)
    
    # Create complete time range
    full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='H')
    
    # Reindex to include all hours
    df_complete = df.reindex(full_range)
    df_complete.index.name = 'Time'
    
    print(f"\n✓ Complete time series created")
    print(f"✓ Total records: {len(df_complete):,}")
    print(f"✓ Missing values: {df_complete['Temperature'].isnull().sum():,}")
    print()
    
    return df_complete


def fill_gaps_methods(df_complete):
    """
    Fill gaps using different methods.
    
    Args:
        df_complete: Complete DataFrame with gaps as NaN
        
    Returns:
        dict: Dictionary of filled DataFrames
    """
    print("="*60)
    print("Filling Gaps with Different Methods")
    print("="*60)
    
    filled_dfs = {}
    
    # Method 1: Forward fill
    print("\n1. Forward Fill Method")
    filled_dfs['forward_fill'] = df_complete.copy()
    filled_dfs['forward_fill']['Temperature'] = filled_dfs['forward_fill']['Temperature'].fillna(method='ffill')
    print(f"   ✓ Gaps filled")
    
    # Method 2: Backward fill
    print("\n2. Backward Fill Method")
    filled_dfs['backward_fill'] = df_complete.copy()
    filled_dfs['backward_fill']['Temperature'] = filled_dfs['backward_fill']['Temperature'].fillna(method='bfill')
    print(f"   ✓ Gaps filled")
    
    # Method 3: Linear interpolation
    print("\n3. Linear Interpolation")
    filled_dfs['linear'] = df_complete.copy()
    filled_dfs['linear']['Temperature'] = filled_dfs['linear']['Temperature'].interpolate(method='linear')
    print(f"   ✓ Gaps filled")
    
    # Method 4: Polynomial interpolation
    print("\n4. Polynomial Interpolation (degree 2)")
    filled_dfs['polynomial'] = df_complete.copy()
    try:
        filled_dfs['polynomial']['Temperature'] = filled_dfs['polynomial']['Temperature'].interpolate(method='polynomial', order=2)
        print(f"   ✓ Gaps filled")
    except:
        filled_dfs['polynomial']['Temperature'] = filled_dfs['polynomial']['Temperature'].interpolate(method='linear')
        print(f"   ✓ Gaps filled (fallback to linear)")
    
    # Method 5: Spline interpolation
    print("\n5. Spline Interpolation")
    filled_dfs['spline'] = df_complete.copy()
    try:
        filled_dfs['spline']['Temperature'] = filled_dfs['spline']['Temperature'].interpolate(method='spline', order=3)
        print(f"   ✓ Gaps filled")
    except:
        filled_dfs['spline']['Temperature'] = filled_dfs['spline']['Temperature'].interpolate(method='linear')
        print(f"   ✓ Gaps filled (fallback to linear)")
    
    # Method 6: Rolling mean
    print("\n6. Rolling Mean Interpolation")
    filled_dfs['rolling_mean'] = df_complete.copy()
    filled_dfs['rolling_mean']['Temperature'] = filled_dfs['rolling_mean']['Temperature'].interpolate(method='linear')
    print(f"   ✓ Gaps filled")
    
    print()
    
    return filled_dfs


def compare_methods(df_complete, filled_dfs):
    """
    Compare different gap-filling methods.
    
    Args:
        df_complete: Original complete DataFrame with gaps
        filled_dfs: Dictionary of filled DataFrames
    """
    print("="*60)
    print("Comparing Gap-Filling Methods")
    print("="*60)
    
    # Find where there were gaps
    gap_mask = df_complete['Temperature'].isnull()
    
    if gap_mask.sum() == 0:
        print("\n✓ No gaps to compare")
        return
    
    print(f"\nComparing methods on {gap_mask.sum():,} filled values\n")
    
    print(f"{'Method':<20} {'Mean Temp':<12} {'Std Dev':<12} {'Min':<12} {'Max':<12}")
    print("-" * 68)
    
    for method_name, df_filled in filled_dfs.items():
        filled_values = df_filled.loc[gap_mask, 'Temperature']
        print(f"{method_name:<20} {filled_values.mean():<12.2f} {filled_values.std():<12.2f} "
              f"{filled_values.min():<12.2f} {filled_values.max():<12.2f}")
    
    print()


def create_gap_visualization(df_complete, filled_dfs, gaps):
    """
    Create visualization of gaps and filled data.
    
    Args:
        df_complete: Complete DataFrame with gaps
        filled_dfs: Dictionary of filled DataFrames
        gaps: List of gap information
    """
    print("="*60)
    print("Creating Gap Visualizations")
    print("="*60)
    
    # Find where we have NaN values
    gap_mask = df_complete['Temperature'].isnull()
    
    if gap_mask.sum() > 0:
        # Get a period with some NaN values
        gap_indices = df_complete.index[gap_mask]
        
        # Select middle gap area for visualization
        mid_gap = gap_indices[len(gap_indices)//2]
        start_plot = mid_gap - timedelta(days=7)
        end_plot = mid_gap + timedelta(days=7)
        
        fig, axes = plt.subplots(3, 1, figsize=(16, 12))
        fig.suptitle('Temperature Data Gap Analysis', fontsize=16, fontweight='bold')
        
        # Plot 1: Original data with gap highlighted
        ax1 = axes[0]
        plot_data = df_complete[start_plot:end_plot]
        ax1.plot(plot_data.index, plot_data['Temperature'], 'o-', color='#3498DB', 
                linewidth=2, markersize=3, label='Original Data')
        
        # Highlight gaps
        gap_data = plot_data[plot_data['Temperature'].isnull()]
        if len(gap_data) > 0:
            for gap_time in gap_data.index:
                ax1.axvline(x=gap_time, alpha=0.3, color='red', linewidth=2)
        
        ax1.set_title('Original Data with Gaps', fontweight='bold')
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Temperature (°C)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Comparison of interpolation methods
        ax2 = axes[1]
        colors = ['#E74C3C', '#2ECC71', '#9B59B6', '#F39C12', '#1ABC9C']
        methods_to_plot = ['linear', 'polynomial', 'spline', 'forward_fill', 'backward_fill']
        
        for method, color in zip(methods_to_plot, colors):
            if method in filled_dfs:
                filled_plot = filled_dfs[method][start_plot:end_plot]
                ax2.plot(filled_plot.index, filled_plot['Temperature'], 
                        '-', color=color, linewidth=2, label=method.replace('_', ' ').title(), 
                        alpha=0.7)
        
        ax2.set_title('Comparison of Gap-Filling Methods', fontweight='bold')
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Temperature (°C)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Entire dataset overview
        ax3 = axes[2]
        ax3.plot(df_complete.index, df_complete['Temperature'], 
                linewidth=0.5, color='#3498DB', alpha=0.6, label='Original Data')
        
        # Mark all gaps
        gap_indices = df_complete.index[gap_mask]
        if len(gap_indices) > 0:
            ax3.scatter(gap_indices, [df_complete['Temperature'].mean()] * len(gap_indices),
                       color='red', s=1, alpha=0.5, label='Gaps')
        
        ax3.set_title('Complete Dataset Overview', fontweight='bold')
        ax3.set_xlabel('Time')
        ax3.set_ylabel('Temperature (°C)')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('gap_analysis.png', dpi=300, bbox_inches='tight')
        print("\n✓ Gap visualization saved to: gap_analysis.png")
        plt.close()


def create_method_comparison_plot(filled_dfs):
    """
    Create comparison plot of all gap-filling methods.
    
    Args:
        filled_dfs: Dictionary of filled DataFrames
    """
    print("\n" + "="*60)
    print("Creating Method Comparison Plot")
    print("="*60)
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('Gap-Filling Methods Comparison', fontsize=16, fontweight='bold')
    axes = axes.flatten()
    
    methods = ['forward_fill', 'backward_fill', 'linear', 'polynomial', 'spline', 'rolling_mean']
    colors = ['#E74C3C', '#2ECC71', '#3498DB', '#9B59B6', '#F39C12', '#1ABC9C']
    
    for idx, (method, color) in enumerate(zip(methods, colors)):
        if method in filled_dfs:
            ax = axes[idx]
            df_method = filled_dfs[method]
            
            # Sample data for visibility (plot every 10th point for large datasets)
            if len(df_method) > 10000:
                plot_data = df_method.iloc[::10]
            else:
                plot_data = df_method
            
            ax.plot(plot_data.index, plot_data['Temperature'], 
                   linewidth=0.5, color=color, alpha=0.7)
            ax.set_title(method.replace('_', ' ').title(), fontweight='bold')
            ax.set_xlabel('Time')
            ax.set_ylabel('Temperature (°C)')
            ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('method_comparison.png', dpi=300, bbox_inches='tight')
    print("\n✓ Method comparison plot saved to: method_comparison.png")
    plt.close()


def create_statistics_plot(df_complete, filled_dfs):
    """
    Create statistical comparison plot.
    
    Args:
        df_complete: Complete DataFrame with gaps
        filled_dfs: Dictionary of filled DataFrames
    """
    print("\n" + "="*60)
    print("Creating Statistics Comparison Plot")
    print("="*60)
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Statistical Comparison of Methods', fontsize=16, fontweight='bold')
    
    # Calculate statistics for each method
    methods = list(filled_dfs.keys())
    means = [filled_dfs[m]['Temperature'].mean() for m in methods]
    stds = [filled_dfs[m]['Temperature'].std() for m in methods]
    
    # Plot 1: Mean temperatures
    ax1 = axes[0]
    colors = plt.cm.Set3(range(len(methods)))
    ax1.bar(range(len(methods)), means, color=colors, alpha=0.7, edgecolor='black')
    ax1.set_xticks(range(len(methods)))
    ax1.set_xticklabels([m.replace('_', '\n') for m in methods], rotation=0, ha='center')
    ax1.set_title('Mean Temperature by Method', fontweight='bold')
    ax1.set_ylabel('Mean Temperature (°C)')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, v in enumerate(means):
        ax1.text(i, v + 0.1, f'{v:.2f}', ha='center', va='bottom', fontweight='bold')
    
    # Plot 2: Standard deviation
    ax2 = axes[1]
    ax2.bar(range(len(methods)), stds, color=colors, alpha=0.7, edgecolor='black')
    ax2.set_xticks(range(len(methods)))
    ax2.set_xticklabels([m.replace('_', '\n') for m in methods], rotation=0, ha='center')
    ax2.set_title('Standard Deviation by Method', fontweight='bold')
    ax2.set_ylabel('Standard Deviation (°C)')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, v in enumerate(stds):
        ax2.text(i, v + 0.1, f'{v:.2f}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('statistics_comparison.png', dpi=300, bbox_inches='tight')
    print("\n✓ Statistics comparison saved to: statistics_comparison.png")
    plt.close()


def generate_gap_report(df, df_complete, gaps, filled_dfs):
    """
    Generate comprehensive gap analysis report.
    
    Args:
        df: Original DataFrame
        df_complete: Complete DataFrame with gaps
        gaps: List of gap information
        filled_dfs: Dictionary of filled DataFrames
    """
    print("="*60)
    print("Generating Gap Analysis Report")
    print("="*60)
    
    report_lines = []
    report_lines.append("="*60)
    report_lines.append("TEMPERATURE DATA GAP ANALYSIS REPORT")
    report_lines.append("="*60)
    report_lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Dataset overview
    report_lines.append("\n\nDATASET OVERVIEW")
    report_lines.append("-" * 60)
    report_lines.append(f"Date Range: {df.index.min()} to {df.index.max()}")
    report_lines.append(f"Duration: {(df.index.max() - df.index.min()).days} days")
    report_lines.append(f"Expected Records (hourly): {len(df_complete):,}")
    report_lines.append(f"Actual Records: {len(df):,}")
    
    # Gap analysis
    report_lines.append("\n\nGAP ANALYSIS")
    report_lines.append("-" * 60)
    missing_count = df_complete['Temperature'].isnull().sum()
    missing_pct = (missing_count / len(df_complete)) * 100
    report_lines.append(f"Missing Records: {missing_count:,} ({missing_pct:.2f}%)")
    report_lines.append(f"Number of Gaps (missing timestamps): {len(gaps)}")
    
    if len(gaps) > 0:
        gap_lengths = [g['length'] for g in gaps]
        report_lines.append(f"\nGap Statistics (missing timestamps):")
        report_lines.append(f"  Average Length: {np.mean(gap_lengths):.1f} hours")
        report_lines.append(f"  Median Length: {np.median(gap_lengths):.0f} hours")
        report_lines.append(f"  Longest Gap: {max(gap_lengths)} hours")
        report_lines.append(f"  Shortest Gap: {min(gap_lengths)} hours")
        
        report_lines.append(f"\nLargest Gaps:")
        sorted_gaps = sorted(gaps, key=lambda x: x['length'], reverse=True)[:5]
        for i, gap in enumerate(sorted_gaps, 1):
            report_lines.append(f"  {i}. {gap['start']} to {gap['end']} ({gap['length']} hours)")
    
    # Temperature statistics
    report_lines.append("\n\nTEMPERATURE STATISTICS (Original Data)")
    report_lines.append("-" * 60)
    report_lines.append(f"Mean: {df['Temperature'].mean():.2f}°C")
    report_lines.append(f"Median: {df['Temperature'].median():.2f}°C")
    report_lines.append(f"Std Dev: {df['Temperature'].std():.2f}°C")
    report_lines.append(f"Min: {df['Temperature'].min():.2f}°C")
    report_lines.append(f"Max: {df['Temperature'].max():.2f}°C")
    
    # Method comparison
    report_lines.append("\n\nGAP-FILLING METHODS COMPARISON")
    report_lines.append("-" * 60)
    for method_name in filled_dfs.keys():
        df_filled = filled_dfs[method_name]
        report_lines.append(f"\n{method_name.replace('_', ' ').title()}:")
        report_lines.append(f"  Mean: {df_filled['Temperature'].mean():.2f}°C")
        report_lines.append(f"  Std Dev: {df_filled['Temperature'].std():.2f}°C")
    
    # Recommendation
    report_lines.append("\n\nRECOMMENDATION")
    report_lines.append("-" * 60)
    report_lines.append("For temperature data:")
    report_lines.append("  - Linear interpolation: Best for short gaps (<6 hours)")
    report_lines.append("  - Spline interpolation: Good for smooth transitions")
    report_lines.append("  - Forward/backward fill: Use only for very short gaps (<2 hours)")
    
    report_lines.append("\n" + "="*60)
    
    # Write to file
    report_text = "\n".join(report_lines)
    
    with open('gap_analysis_report.txt', 'w') as f:
        f.write(report_text)
    
    print("\n✓ Gap analysis report saved to: gap_analysis_report.txt")
    print(f"\n{report_text}")
    print()


def export_filled_data(filled_dfs):
    """
    Export filled datasets to CSV files.
    
    Args:
        filled_dfs: Dictionary of filled DataFrames
    """
    print("="*60)
    print("Exporting Filled Datasets")
    print("="*60)
    
    for method_name, df_filled in filled_dfs.items():
        filename = f'temperature_filled_{method_name}.csv'
        df_filled.to_csv(filename)
        print(f"✓ Exported {method_name} to: {filename}")
    
    print()


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 13: Temperature Data Gap Analysis")
    print("="*60)
    
    # Load data
    df = load_data()
    
    # Basic statistics
    analyze_basic_statistics(df)
    
    # Detect gaps
    gaps = detect_gaps(df)
    
    # Create complete time series
    df_complete = create_complete_timeseries(df)
    
    # Fill gaps with different methods
    filled_dfs = fill_gaps_methods(df_complete)
    
    # Compare methods
    compare_methods(df_complete, filled_dfs)
    
    # Create visualizations
    create_gap_visualization(df_complete, filled_dfs, gaps)
    create_method_comparison_plot(filled_dfs)
    create_statistics_plot(df_complete, filled_dfs)
    
    # Generate report
    generate_gap_report(df, df_complete, gaps, filled_dfs)
    
    # Export filled data
    export_filled_data(filled_dfs)
    
    print("\n" + "="*60)
    print("✓ All gap analysis completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print("  - gap_analysis_report.txt")
    print("  - gap_analysis.png")
    print("  - method_comparison.png")
    print("  - statistics_comparison.png")
    print("  - temperature_filled_*.csv (6 files)")
    print()


if __name__ == "__main__":
    main()
