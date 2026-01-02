"""
Exercise 03: Sales Data Visualization
Author: Sherif Elashmawy
Date: January 2026

This script uses the company database from Exercise 2 to:
1. Query daily total sales from the orders table
2. Create a line plot of daily sales using matplotlib
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os

# Path to database from Exercise 02
DB_PATH = '../exercise-02/company_database.db'


def check_database_exists():
    """
    Check if the database from Exercise 02 exists.
    
    Returns:
        bool: True if database exists, False otherwise
    """
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database not found at {DB_PATH}")
        print("Please run Exercise 02 first to create the database.")
        return False
    
    print(f"✓ Database found: {DB_PATH}\n")
    return True


def explore_orders_table(conn):
    """
    Explore the structure and sample data from the orders table.
    
    Args:
        conn: SQLite database connection
    """
    print("="*60)
    print("Exploring Orders Table Structure")
    print("="*60)
    
    # Get table schema
    schema_query = "PRAGMA table_info(orders)"
    schema_df = pd.read_sql_query(schema_query, conn)
    
    print("\nTable Schema:")
    print(schema_df.to_string(index=False))
    
    # Get sample data
    sample_query = "SELECT * FROM orders LIMIT 5"
    sample_df = pd.read_sql_query(sample_query, conn)
    
    print("\n\nSample Data (first 5 rows):")
    print(sample_df.to_string(index=False))
    
    # Get total number of orders
    count_query = "SELECT COUNT(*) as total_orders FROM orders"
    count_df = pd.read_sql_query(count_query, conn)
    
    print(f"\n\nTotal Orders in Database: {count_df['total_orders'][0]}")
    print()


def query_daily_sales(conn):
    """
    Query daily total sales amount from orders table.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        pd.DataFrame: Daily sales data with date and total_sales columns
    """
    print("="*60)
    print("Querying Daily Total Sales")
    print("="*60)
    
    # Query to calculate daily total sales
    # Using 'amount' column from the orders table
    query = """
    SELECT 
        DATE(order_date) as date,
        SUM(amount) as total_sales,
        COUNT(*) as num_orders,
        ROUND(AVG(amount), 2) as avg_order_value
    FROM orders
    GROUP BY DATE(order_date)
    ORDER BY date
    """
    
    df = pd.read_sql_query(query, conn)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    print(f"\n✓ Query executed successfully")
    print(f"✓ Retrieved {len(df)} days of sales data")
    print(f"\nDate Range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    print(f"Total Sales: ${df['total_sales'].sum():,.2f}")
    print(f"Average Daily Sales: ${df['total_sales'].mean():,.2f}")
    print(f"Highest Daily Sales: ${df['total_sales'].max():,.2f} on {df.loc[df['total_sales'].idxmax(), 'date'].strftime('%Y-%m-%d')}")
    print(f"Lowest Daily Sales: ${df['total_sales'].min():,.2f} on {df.loc[df['total_sales'].idxmin(), 'date'].strftime('%Y-%m-%d')}")
    
    print("\n\nFirst 10 days of sales:")
    print(df.head(10).to_string(index=False))
    print()
    
    return df


def create_sales_visualization(df, output_file='daily_sales_plot.png'):
    """
    Create a line plot of daily sales using matplotlib.
    
    Args:
        df: DataFrame with date and total_sales columns
        output_file: Filename for the output plot
    """
    print("="*60)
    print("Creating Sales Visualization")
    print("="*60)
    
    # Create figure with larger size for better readability
    plt.figure(figsize=(14, 7))
    
    # Create the line plot
    plt.plot(df['date'], df['total_sales'], 
            linewidth=2, 
            color='#2E86AB',
            marker='o',
            markersize=4,
            markerfacecolor='#A23B72',
            markeredgecolor='white',
            markeredgewidth=0.5)
    
    # Customize the plot
    plt.title('Daily Sales Over Time', 
             fontsize=16, 
             fontweight='bold', 
             pad=20)
    
    plt.xlabel('Date', fontsize=12, fontweight='bold')
    plt.ylabel('Total Sales ($)', fontsize=12, fontweight='bold')
    
    # Format y-axis to show currency
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    # Add grid for better readability
    plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    # Add some statistics as text
    total_sales = df['total_sales'].sum()
    avg_sales = df['total_sales'].mean()
    
    stats_text = f'Total Sales: ${total_sales:,.2f}\nAvg Daily Sales: ${avg_sales:,.2f}'
    plt.text(0.02, 0.98, stats_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    # Tight layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Plot saved to: {output_file}")
    
    plt.close()


def create_advanced_visualization(df, output_file='sales_analysis_dashboard.png'):
    """
    Create a more detailed dashboard with multiple visualizations.
    
    Args:
        df: DataFrame with sales data
        output_file: Filename for the output plot
    """
    print("\n" + "="*60)
    print("Creating Advanced Sales Dashboard")
    print("="*60)
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Sales Analysis Dashboard', fontsize=18, fontweight='bold', y=1.00)
    
    # Plot 1: Daily Sales Line Chart
    ax1 = axes[0, 0]
    ax1.plot(df['date'], df['total_sales'], linewidth=2, color='#2E86AB')
    ax1.fill_between(df['date'], df['total_sales'], alpha=0.3, color='#2E86AB')
    ax1.set_title('Daily Sales Trend', fontweight='bold')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Total Sales ($)')
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Number of Orders per Day
    ax2 = axes[0, 1]
    ax2.bar(df['date'], df['num_orders'], color='#A23B72', alpha=0.7)
    ax2.set_title('Number of Orders per Day', fontweight='bold')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Number of Orders')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.tick_params(axis='x', rotation=45)
    
    # Plot 3: Average Order Value
    ax3 = axes[1, 0]
    ax3.plot(df['date'], df['avg_order_value'], 
            linewidth=2, 
            color='#F18F01',
            marker='s',
            markersize=4)
    ax3.set_title('Average Order Value Over Time', fontweight='bold')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('Avg Order Value ($)')
    ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    
    # Plot 4: Sales Distribution Histogram
    ax4 = axes[1, 1]
    ax4.hist(df['total_sales'], bins=20, color='#06A77D', alpha=0.7, edgecolor='black')
    ax4.set_title('Distribution of Daily Sales', fontweight='bold')
    ax4.set_xlabel('Daily Sales ($)')
    ax4.set_ylabel('Frequency (Days)')
    ax4.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Dashboard saved to: {output_file}")
    
    plt.close()


def export_to_csv(df, filename='daily_sales_data.csv'):
    """
    Export daily sales data to CSV file.
    
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
    print("Exercise 03: Sales Data Visualization")
    print("="*60 + "\n")
    
    # Check if database exists
    if not check_database_exists():
        return
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    # Explore the orders table
    explore_orders_table(conn)
    
    # Query daily sales
    df_sales = query_daily_sales(conn)
    
    # Create visualizations
    create_sales_visualization(df_sales)
    create_advanced_visualization(df_sales)
    
    # Export data
    export_to_csv(df_sales)
    
    # Close database connection
    conn.close()
    
    print("\n" + "="*60)
    print("✓ All tasks completed successfully!")
    print("="*60 + "\n")
    
    print("Generated Files:")
    print("  - daily_sales_plot.png")
    print("  - sales_analysis_dashboard.png")
    print("  - daily_sales_data.csv")
    print()


if __name__ == "__main__":
    main()