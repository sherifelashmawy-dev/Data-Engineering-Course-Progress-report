"""
Exercise 02: SQL Database Operations
Author: Sherif Elashmawy
Date: January 2026

This script performs SQL database operations on company data:
a) Create SQLite database from CSV files
b) Query and join employee_projects with employees_realistic
c) Calculate salary costs per project
d) Join with project budgets
e) Calculate budget utilization percentage
"""

import sqlite3
import pandas as pd
import os

# Database name
DB_NAME = 'company_database.db'

# CSV files to import
CSV_FILES = [
    'employees_realistic.csv',
    'employee_projects.csv',
    'projects.csv',
    'departments.csv',
    'customers.csv',
    'orders.csv',
    'order_items.csv'
]


def create_database_from_csv():
    """
    Task a) Create SQL database where each CSV file appears as a table.
    
    Returns:
        sqlite3.Connection: Database connection object
    """
    print("\n" + "="*60)
    print("Task a) Creating SQLite Database from CSV Files")
    print("="*60)
    
    # Remove existing database if it exists
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        print(f"✓ Removed existing database: {DB_NAME}")
    
    # Create connection to SQLite database
    conn = sqlite3.connect(DB_NAME)
    print(f"✓ Created new database: {DB_NAME}")
    
    # Import each CSV file as a table
    for csv_file in CSV_FILES:
        if os.path.exists(csv_file):
            # Table name is CSV filename without extension
            table_name = csv_file.replace('.csv', '')
            
            # Read CSV into DataFrame
            df = pd.read_csv(csv_file)
            
            # Write DataFrame to SQL table
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            
            print(f"✓ Imported {csv_file} → Table '{table_name}' ({len(df)} rows)")
        else:
            print(f"⚠ Warning: {csv_file} not found, skipping...")
    
    print(f"\n✓ Database created successfully\n")
    
    return conn


def query_employee_project_join(conn):
    """
    Task b) Query project_id, employee_id, hours_worked from employee_projects
    and join with employee_id, salaries from employees_realistic.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        pd.DataFrame: Joined query results
    """
    print("="*60)
    print("Task b) Join Employee Projects with Employee Salaries")
    print("="*60)
    
    query = """
    SELECT 
        ep.project_id,
        ep.employee_id,
        ep.hours_worked,
        e.first_name || ' ' || e.last_name as name,
        e.salary,
        e.position,
        e.department_id
    FROM employee_projects ep
    INNER JOIN employees_realistic e 
        ON ep.employee_id = e.employee_id
    ORDER BY ep.project_id, ep.employee_id
    """
    
    df = pd.read_sql_query(query, conn)
    
    print(f"\n✓ Query executed successfully: {len(df)} records retrieved\n")
    print(df.to_string(index=False))
    print()
    
    return df


def calculate_salary_costs_per_project(conn):
    """
    Task c) Calculate salary costs per project.
    Assumption: Annual salary, 1900 working hours per year.
    
    Formula: Hourly rate = Annual salary / 1900
             Project cost = Hourly rate × Hours worked
    
    Args:
        conn: SQLite database connection
        
    Returns:
        pd.DataFrame: Salary costs per project
    """
    print("="*60)
    print("Task c) Calculate Salary Costs Per Project")
    print("="*60)
    
    query = """
    SELECT 
        ep.project_id,
        ep.employee_id,
        e.first_name || ' ' || e.last_name as name,
        e.salary,
        ep.hours_worked,
        ROUND((e.salary / 1900.0) * ep.hours_worked, 2) as project_cost
    FROM employee_projects ep
    INNER JOIN employees_realistic e 
        ON ep.employee_id = e.employee_id
    ORDER BY ep.project_id, project_cost DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    print(f"\n✓ Individual employee costs calculated\n")
    print(df.to_string(index=False))
    
    # Aggregate costs per project
    print("\n" + "-"*60)
    print("Total Salary Costs Per Project:")
    print("-"*60)
    
    summary_query = """
    SELECT 
        ep.project_id,
        COUNT(DISTINCT ep.employee_id) as num_employees,
        SUM(ep.hours_worked) as total_hours,
        ROUND(SUM((e.salary / 1900.0) * ep.hours_worked), 2) as total_salary_cost
    FROM employee_projects ep
    INNER JOIN employees_realistic e 
        ON ep.employee_id = e.employee_id
    GROUP BY ep.project_id
    ORDER BY ep.project_id
    """
    
    summary_df = pd.read_sql_query(summary_query, conn)
    print()
    print(summary_df.to_string(index=False))
    print()
    
    return summary_df


def join_with_project_budget(conn):
    """
    Task d) Join salary costs with project budgets.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        pd.DataFrame: Projects with costs and budgets
    """
    print("="*60)
    print("Task d) Join Salary Costs with Project Budgets")
    print("="*60)
    
    query = """
    SELECT 
        p.project_id,
        p.project_name,
        p.budget,
        COUNT(DISTINCT ep.employee_id) as num_employees,
        SUM(ep.hours_worked) as total_hours,
        ROUND(SUM((e.salary / 1900.0) * ep.hours_worked), 2) as total_salary_cost
    FROM projects p
    INNER JOIN employee_projects ep 
        ON p.project_id = ep.project_id
    INNER JOIN employees_realistic e 
        ON ep.employee_id = e.employee_id
    GROUP BY p.project_id, p.project_name, p.budget
    ORDER BY p.project_id
    """
    
    df = pd.read_sql_query(query, conn)
    
    print(f"\n✓ Query executed successfully\n")
    print(df.to_string(index=False))
    print()
    
    return df


def calculate_budget_utilization(conn):
    """
    Task e) Calculate what fraction of project budget the salary costs represent.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        pd.DataFrame: Budget utilization analysis
    """
    print("="*60)
    print("Task e) Calculate Budget Utilization Percentage")
    print("="*60)
    
    query = """
    SELECT 
        p.project_id,
        p.project_name,
        p.budget,
        COUNT(DISTINCT ep.employee_id) as num_employees,
        SUM(ep.hours_worked) as total_hours,
        ROUND(SUM((e.salary / 1900.0) * ep.hours_worked), 2) as total_salary_cost,
        ROUND((SUM((e.salary / 1900.0) * ep.hours_worked) / p.budget) * 100, 2) as budget_utilization_pct,
        ROUND(p.budget - SUM((e.salary / 1900.0) * ep.hours_worked), 2) as remaining_budget
    FROM projects p
    INNER JOIN employee_projects ep 
        ON p.project_id = ep.project_id
    INNER JOIN employees_realistic e 
        ON ep.employee_id = e.employee_id
    GROUP BY p.project_id, p.project_name, p.budget
    ORDER BY budget_utilization_pct DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    print(f"\n✓ Budget utilization calculated for {len(df)} projects\n")
    print(df.to_string(index=False))
    
    # Summary statistics
    print("\n" + "-"*60)
    print("Summary Statistics:")
    print("-"*60)
    print(f"Average Budget Utilization: {df['budget_utilization_pct'].mean():.2f}%")
    print(f"Highest Utilization: {df['budget_utilization_pct'].max():.2f}% (Project {df.loc[df['budget_utilization_pct'].idxmax(), 'project_name']})")
    print(f"Lowest Utilization: {df['budget_utilization_pct'].min():.2f}% (Project {df.loc[df['budget_utilization_pct'].idxmin(), 'project_name']})")
    print(f"Total Budget: ${df['budget'].sum():,.2f}")
    print(f"Total Salary Costs: ${df['total_salary_cost'].sum():,.2f}")
    print(f"Total Remaining: ${df['remaining_budget'].sum():,.2f}")
    print()
    
    return df


def export_results_to_csv(df, filename):
    """
    Export query results to CSV file.
    
    Args:
        df: DataFrame to export
        filename: Output CSV filename
    """
    df.to_csv(filename, index=False)
    print(f"✓ Results exported to {filename}")


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 02: SQL Database Operations")
    print("="*60)
    
    # Task a) Create database
    conn = create_database_from_csv()
    
    # Task b) Join employees and projects
    df_join = query_employee_project_join(conn)
    
    # Task c) Calculate salary costs per project
    df_costs = calculate_salary_costs_per_project(conn)
    
    # Task d) Join with project budgets
    df_budget = join_with_project_budget(conn)
    
    # Task e) Calculate budget utilization
    df_utilization = calculate_budget_utilization(conn)
    
    # Export final results
    export_results_to_csv(df_utilization, 'project_budget_analysis.csv')
    
    # Close database connection
    conn.close()
    
    print("\n" + "="*60)
    print("✓ All tasks completed successfully!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()