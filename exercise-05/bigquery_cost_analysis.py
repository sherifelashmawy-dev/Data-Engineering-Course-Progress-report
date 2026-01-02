"""
Exercise 05: BigQuery Storage Cost Estimation
Author: Sherif Elashmawy
Date: January 2026

This script estimates BigQuery storage costs over 5 years for a company that:
- Collects 5 GB of data every day
- Keeps all old data (no deletion)

Based on BigQuery pricing: https://cloud.google.com/bigquery/pricing#storage
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# BigQuery Storage Pricing (as of January 2026)
# Source: https://cloud.google.com/bigquery/pricing#storage

# Active storage: First 10 GB free, then $0.02 per GB per month
ACTIVE_STORAGE_PRICE_PER_GB_MONTH = 0.02
FREE_TIER_GB = 10

# Long-term storage (90+ days old): $0.01 per GB per month
LONG_TERM_STORAGE_PRICE_PER_GB_MONTH = 0.01
LONG_TERM_THRESHOLD_DAYS = 90

# Company data collection rate
DAILY_DATA_GB = 5
YEARS = 5


def calculate_storage_costs():
    """
    Calculate BigQuery storage costs over 5 years.
    
    Scenario:
    - Company collects 5 GB per day
    - All data is kept (no deletion)
    - Data older than 90 days moves to long-term storage pricing
    
    Returns:
        pd.DataFrame: Monthly cost breakdown
    """
    print("\n" + "="*60)
    print("BigQuery Storage Cost Analysis")
    print("="*60)
    print(f"\nScenario:")
    print(f"  - Daily data collection: {DAILY_DATA_GB} GB/day")
    print(f"  - Time period: {YEARS} years")
    print(f"  - Data retention: Keep all data (no deletion)")
    print(f"\nBigQuery Pricing:")
    print(f"  - Active storage (0-90 days): ${ACTIVE_STORAGE_PRICE_PER_GB_MONTH}/GB/month")
    print(f"  - Long-term storage (90+ days): ${LONG_TERM_STORAGE_PRICE_PER_GB_MONTH}/GB/month")
    print(f"  - Free tier: First {FREE_TIER_GB} GB")
    print("="*60 + "\n")
    
    # Calculate for each month
    months = YEARS * 12
    monthly_data = []
    
    for month in range(1, months + 1):
        # Days in this month (approximate as 30.44 days average)
        days_in_month = 30.44
        
        # Total days elapsed
        total_days = month * days_in_month
        
        # Total data accumulated (GB)
        total_data_gb = total_days * DAILY_DATA_GB
        
        # Data in active storage (last 90 days)
        days_active = min(90, total_days)
        active_storage_gb = days_active * DAILY_DATA_GB
        
        # Data in long-term storage (older than 90 days)
        if total_days > 90:
            long_term_storage_gb = total_data_gb - active_storage_gb
        else:
            long_term_storage_gb = 0
        
        # Calculate costs (subtract free tier from active storage)
        active_billable_gb = max(0, active_storage_gb - FREE_TIER_GB)
        active_cost = active_billable_gb * ACTIVE_STORAGE_PRICE_PER_GB_MONTH
        long_term_cost = long_term_storage_gb * LONG_TERM_STORAGE_PRICE_PER_GB_MONTH
        total_monthly_cost = active_cost + long_term_cost
        
        monthly_data.append({
            'month': month,
            'year': (month - 1) // 12 + 1,
            'total_data_gb': round(total_data_gb, 2),
            'active_storage_gb': round(active_storage_gb, 2),
            'long_term_storage_gb': round(long_term_storage_gb, 2),
            'active_cost': round(active_cost, 2),
            'long_term_cost': round(long_term_cost, 2),
            'total_monthly_cost': round(total_monthly_cost, 2)
        })
    
    df = pd.DataFrame(monthly_data)
    
    return df


def display_summary(df):
    """
    Display cost summary and key metrics.
    
    Args:
        df: DataFrame with monthly cost data
    """
    print("="*60)
    print("Cost Summary")
    print("="*60)
    
    # Final totals (end of year 5)
    final_row = df.iloc[-1]
    
    print(f"\nEnd of Year 5 Status:")
    print(f"  - Total data accumulated: {final_row['total_data_gb']:,.2f} GB")
    print(f"  - Active storage: {final_row['active_storage_gb']:,.2f} GB")
    print(f"  - Long-term storage: {final_row['long_term_storage_gb']:,.2f} GB")
    print(f"  - Monthly cost (Year 5): ${final_row['total_monthly_cost']:,.2f}")
    
    # Total costs over 5 years
    total_5_year_cost = df['total_monthly_cost'].sum()
    avg_monthly_cost = df['total_monthly_cost'].mean()
    
    print(f"\n5-Year Totals:")
    print(f"  - Total cost over 5 years: ${total_5_year_cost:,.2f}")
    print(f"  - Average monthly cost: ${avg_monthly_cost:,.2f}")
    print(f"  - Average annual cost: ${avg_monthly_cost * 12:,.2f}")
    
    # Yearly breakdown
    print(f"\nYearly Cost Breakdown:")
    for year in range(1, YEARS + 1):
        year_data = df[df['year'] == year]
        year_cost = year_data['total_monthly_cost'].sum()
        year_end_data = year_data['total_data_gb'].iloc[-1]
        print(f"  Year {year}: ${year_cost:,.2f} (ending with {year_end_data:,.2f} GB)")
    
    print()


def display_monthly_details(df, sample_months=[1, 12, 24, 36, 48, 60]):
    """
    Display detailed breakdown for sample months.
    
    Args:
        df: DataFrame with monthly cost data
        sample_months: List of months to display details for
    """
    print("="*60)
    print("Sample Monthly Breakdown")
    print("="*60 + "\n")
    
    for month in sample_months:
        if month <= len(df):
            row = df[df['month'] == month].iloc[0]
            print(f"Month {month} (Year {row['year']}):")
            print(f"  Total Data: {row['total_data_gb']:,.2f} GB")
            print(f"  Active Storage: {row['active_storage_gb']:,.2f} GB → ${row['active_cost']:.2f}")
            print(f"  Long-term Storage: {row['long_term_storage_gb']:,.2f} GB → ${row['long_term_cost']:.2f}")
            print(f"  Monthly Cost: ${row['total_monthly_cost']:.2f}")
            print()


def create_visualizations(df):
    """
    Create visualizations of cost growth and storage distribution.
    
    Args:
        df: DataFrame with cost data
    """
    print("="*60)
    print("Creating Visualizations")
    print("="*60)
    
    # Create a figure with multiple subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('BigQuery Storage Cost Analysis - 5 Year Projection', 
                 fontsize=16, fontweight='bold')
    
    # Plot 1: Total Data Growth
    ax1 = axes[0, 0]
    ax1.plot(df['month'], df['total_data_gb'], linewidth=2, color='#2E86AB')
    ax1.fill_between(df['month'], df['total_data_gb'], alpha=0.3, color='#2E86AB')
    ax1.set_title('Total Data Accumulation Over Time', fontweight='bold')
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Total Data (GB)')
    ax1.grid(True, alpha=0.3)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Plot 2: Monthly Cost Growth
    ax2 = axes[0, 1]
    ax2.plot(df['month'], df['total_monthly_cost'], linewidth=2, color='#A23B72', marker='o', markersize=2)
    ax2.set_title('Monthly Storage Cost Over Time', fontweight='bold')
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Monthly Cost ($)')
    ax2.grid(True, alpha=0.3)
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    # Plot 3: Storage Type Distribution
    ax3 = axes[1, 0]
    ax3.stackplot(df['month'], 
                  df['active_storage_gb'], 
                  df['long_term_storage_gb'],
                  labels=['Active Storage (0-90 days)', 'Long-term Storage (90+ days)'],
                  colors=['#F18F01', '#06A77D'],
                  alpha=0.8)
    ax3.set_title('Storage Distribution by Type', fontweight='bold')
    ax3.set_xlabel('Month')
    ax3.set_ylabel('Storage (GB)')
    ax3.legend(loc='upper left')
    ax3.grid(True, alpha=0.3)
    ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Plot 4: Cost Breakdown
    ax4 = axes[1, 1]
    ax4.stackplot(df['month'],
                  df['active_cost'],
                  df['long_term_cost'],
                  labels=['Active Storage Cost', 'Long-term Storage Cost'],
                  colors=['#F18F01', '#06A77D'],
                  alpha=0.8)
    ax4.set_title('Cost Breakdown by Storage Type', fontweight='bold')
    ax4.set_xlabel('Month')
    ax4.set_ylabel('Monthly Cost ($)')
    ax4.legend(loc='upper left')
    ax4.grid(True, alpha=0.3)
    ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    plt.tight_layout()
    plt.savefig('bigquery_cost_analysis.png', dpi=300, bbox_inches='tight')
    print("\n✓ Visualization saved to: bigquery_cost_analysis.png\n")
    plt.close()


def export_to_csv(df, filename='bigquery_cost_estimate.csv'):
    """
    Export cost analysis to CSV.
    
    Args:
        df: DataFrame to export
        filename: Output filename
    """
    df.to_csv(filename, index=False)
    print(f"✓ Cost analysis exported to: {filename}")


def calculate_alternative_scenarios(df):
    """
    Calculate alternative cost scenarios.
    """
    print("="*60)
    print("Alternative Scenarios")
    print("="*60)
    
    final_data_gb = df.iloc[-1]['total_data_gb']
    
    # Scenario 1: All data in active storage (no long-term benefit)
    all_active_cost_monthly = (final_data_gb - FREE_TIER_GB) * ACTIVE_STORAGE_PRICE_PER_GB_MONTH
    all_active_cost_5year = df['month'].count() * ((df['total_data_gb'].mean() - FREE_TIER_GB) * ACTIVE_STORAGE_PRICE_PER_GB_MONTH)
    
    # Scenario 2: Data deletion after 1 year (only keep 365 days)
    data_365_days = 365 * DAILY_DATA_GB
    deletion_cost_monthly = (data_365_days - FREE_TIER_GB) * ACTIVE_STORAGE_PRICE_PER_GB_MONTH
    deletion_cost_5year = 60 * deletion_cost_monthly
    
    # Actual scenario
    actual_5year = df['total_monthly_cost'].sum()
    
    print(f"\nScenario Comparison (5-year total costs):")
    print(f"\n1. Current Scenario (Keep all data):")
    print(f"   - 5-year total: ${actual_5year:,.2f}")
    
    print(f"\n2. If no long-term storage pricing existed (all active):")
    print(f"   - 5-year total: ${all_active_cost_5year:,.2f}")
    print(f"   - Savings from long-term pricing: ${all_active_cost_5year - actual_5year:,.2f}")
    
    print(f"\n3. If data deleted after 1 year:")
    print(f"   - 5-year total: ${deletion_cost_5year:,.2f}")
    print(f"   - Savings from deletion: ${actual_5year - deletion_cost_5year:,.2f}")
    
    print()


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 05: BigQuery Storage Cost Estimation")
    print("="*60)
    
    # Calculate costs
    df_costs = calculate_storage_costs()
    
    # Display summary
    display_summary(df_costs)
    
    # Display monthly details
    display_monthly_details(df_costs)
    
    # Calculate alternative scenarios
    calculate_alternative_scenarios(df_costs)
    
    # Create visualizations
    create_visualizations(df_costs)
    
    # Export to CSV
    export_to_csv(df_costs)
    
    print("\n" + "="*60)
    print("✓ All calculations completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print("  - bigquery_cost_analysis.png")
    print("  - bigquery_cost_estimate.csv")
    print()


if __name__ == "__main__":
    main()
