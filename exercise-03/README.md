# Exercise 03: Sales Data Visualization

## Objective
Use the company database from Exercise 02 to query daily sales totals and create visualizations using matplotlib.

## Data Source
- Uses `company_database.db` from Exercise 02
- Analyzes the `orders` table

## Tasks Completed

### 1. Database Connection
✅ Connected to SQLite database from Exercise 02

### 2. Daily Sales Query
✅ Queried and aggregated sales by date
✅ Calculated daily totals, order counts, and average order values

### 3. Visualizations
✅ Created line plot of daily sales over time
✅ Built comprehensive dashboard with 4 charts:
- Daily sales trend with area fill
- Number of orders per day (bar chart)
- Average order value over time
- Distribution of daily sales (histogram)

## Requirements
```bash
pip install -r requirements.txt
```

## Usage
```bash
# Must run Exercise 02 first to create the database
cd ../exercise-02
python company_operations.py

# Then run this exercise
cd ../exercise-03
python sales_visualization.py
```

## Output Files
- `daily_sales_plot.png` - Simple line chart
- `sales_analysis_dashboard.png` - Multi-chart dashboard
- `daily_sales_data.csv` - Exported sales data

## Key Insights
- Visualizes sales trends over time
- Identifies peak and low sales days
- Shows order volume patterns
- Analyzes average order value trends

---

**Author:** Sherif Elashmawy  
**Date:** January 2026