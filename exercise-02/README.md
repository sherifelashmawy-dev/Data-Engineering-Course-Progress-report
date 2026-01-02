# Exercise 02: SQL Database Operations

## Objective
Work with company data using SQLite to perform database operations, joins, and business calculations.

## Data Files
- `employees_realistic.csv` - Employee information
- `employee_projects.csv` - Project assignments and hours worked
- `projects.csv` - Project details and budgets
- `departments.csv` - Department information
- `customers.csv` - Customer data
- `orders.csv` - Order information
- `order_items.csv` - Order line items

## Tasks Completed

### a) Database Creation
✅ Created SQLite database from 7 CSV files

### b) Employee-Project Join
✅ Joined employee_projects with employees_realistic

### c) Salary Cost Calculation
✅ Calculated costs using: (Salary / 1900) × Hours Worked

### d) Budget Integration
✅ Joined salary costs with project budgets

### e) Budget Utilization
✅ Calculated percentage of budget used

## Usage
```bash
pip install -r requirements.txt
python company_operations.py
```

## Output
- `company_database.db` - SQLite database
- `project_budget_analysis.csv` - Results

---

**Author:** Sherif Elashmawy  
**Date:** January 2026