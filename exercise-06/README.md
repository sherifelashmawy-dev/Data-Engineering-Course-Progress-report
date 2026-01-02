# Exercise 06: World Bank Population Data Analysis

## Objective
Query World Bank population data from BigQuery, transform it, and create a Nordic countries population table.

## Data Source
- **Dataset**: `bigquery-public-data.world_bank_global_population.population_by_country`
- **Description**: Historical population data for all countries
- **Coverage**: Multiple decades of population statistics

## Tasks Completed

### 1. Query Entire Dataset
✅ Downloaded complete World Bank population dataset using BigQuery API  
✅ Retrieved data for all countries and years  

### 2. Data Transformation
✅ Transformed from long format (country, year, population) to wide format  
✅ Created structure: year | Country1 | Country2 | Country3 | ...  

### 3. Nordic Countries Table
✅ Extracted data for Nordic countries: Finland, Sweden, Norway, Denmark, Iceland  
✅ Created table with columns: ['year', 'Finland', 'Sweden', 'Norway', 'Denmark', 'Iceland']  

### 4. Upload to BigQuery
✅ Uploaded transformed Nordic data back to BigQuery using API  
✅ Created table in custom dataset  
✅ Verified upload by querying back  

### 5. Analysis
✅ Calculated population growth statistics  
✅ Identified trends for each Nordic country  
✅ Computed total Nordic population  

## Requirements
```bash
pip install -r requirements.txt
```

## Google Cloud Setup
- Project: data-analytics-project-482302
- BigQuery API enabled
- Credentials configured

## Usage
```bash
python world_bank_population.py
```

## Output
- **BigQuery Table**: `data-analytics-project-482302.world_bank_analysis.nordic_population`
- **CSV File**: `nordic_population.csv`

## Data Format

### Original (Long Format)
| country_name | year | population |
|--------------|------|------------|
| Finland | 2020 | 5,500,000 |
| Sweden | 2020 | 10,300,000 |

### Transformed (Wide Format)
| year | Finland | Sweden | Norway | Denmark | Iceland |
|------|---------|--------|--------|---------|---------|
| 2020 | 5.5M | 10.3M | 5.4M | 5.8M | 0.4M |

## Key Insights
- Population trends for Nordic countries
- Growth rates over time
- Total Nordic population
- Historical demographic changes

---

**Author:** Sherif Elashmawy  
**Date:** January 2026