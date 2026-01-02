# Exercise 08: Google Analytics 4 Data Analysis

## Objective
Analyze Google Analytics 4 e-commerce data using BigQuery, focusing on user behavior and revenue metrics during a specific date range.

## Data Source
- **Dataset**: `bigquery-public-data.ga4_obfuscated_sample_ecommerce`
- **Date Range**: 2021-01-15 to 2021-01-31
- **Type**: Wildcard table using `_TABLE_SUFFIX`

## Tasks Completed

### a) Query Raw GA4 Data
✅ Used wildcard table with `_TABLE_SUFFIX` filter  
✅ Queried user_pseudo_id, event counts, and revenue  
✅ Aggregated metrics per user  

### b) Save to BigQuery (Action Query)
✅ Created destination table using CREATE TABLE AS  
✅ Data stayed in BigQuery (no local download)  
✅ Saved to custom dataset: `G4_daily_user.G4_daily_user_data`  

### c) Import to Pandas
✅ Queried created table  
✅ Imported to local pandas DataFrame  
✅ Analyzed data structure and statistics  

### d) Visualize Top Customers
✅ Created bar chart of top 20 customers by revenue  
✅ Added activity level comparison  
✅ Built comprehensive dashboard  

## Requirements
```bash
pip install -r requirements.txt
```

## Usage

### Python Script:
```bash
python ga4_analysis.py
```

### Jupyter Notebook:
```bash
jupyter notebook ga4_analysis.ipynb
```

## Outputs
- **BigQuery Table**: `data-analytics-project-482302.G4_daily_user.G4_daily_user_data`
- **Top 20 Chart**: `top_20_customers_revenue.png`
- **Dashboard**: `ga4_analysis_dashboard.png`
- **CSV Export**: `ga4_user_data.csv`

## Key Insights
- Total users with revenue during the period
- Revenue distribution among users
- Correlation between activity and revenue
- Top spending customers

## Technical Highlights
- Wildcard table querying with `_TABLE_SUFFIX`
- UNNEST operations for nested GA4 event parameters
- Action query for efficient BigQuery processing
- Multi-chart dashboard visualization

---

**Author:** Sherif Elashmawy  
**Date:** January 2026