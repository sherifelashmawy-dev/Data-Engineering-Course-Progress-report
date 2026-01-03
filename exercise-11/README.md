# Exercise 11: Data Quality and Profiling Analysis

## Objective
Perform comprehensive data quality analysis and profiling on the cars dataset to identify issues and generate quality metrics.

## Data Source
- **File**: cars.csv
- **Description**: Vehicle registration data with attributes like brand, engine volume, cylinders, transmission
- **Columns**: VehicleClass, RegDate, EngineVolume, Cylinders, Brand, Transmission, Year

## Tasks Completed

### 1. Data Loading and Overview
✅ Loaded dataset and displayed basic statistics  
✅ Analyzed column types and distributions  

### 2. Missing Value Analysis
✅ Identified columns with missing values  
✅ Calculated missing percentages  
✅ Created missing value visualizations  

### 3. Duplicate Detection
✅ Detected exact duplicates  
✅ Checked key column duplicates  

### 4. Outlier Detection
✅ Applied IQR method for outlier detection  
✅ Identified outliers in numeric columns  
✅ Calculated valid ranges  

### 5. Data Type Validation
✅ Verified data types  
✅ Identified potential categorical columns  
✅ Validated date columns  

### 6. Visualizations
✅ Missing values bar chart and heatmap  
✅ Distribution plots for numeric columns  
✅ Correlation heatmap  

### 7. Quality Report
✅ Generated comprehensive quality report  
✅ Calculated data quality score  
✅ Provided quality rating  

## Requirements
```bash
pip install -r requirements.txt
```

## Usage
```bash
python data_quality_analysis.py
```

## Outputs
- `data_quality_report.txt` - Comprehensive quality report
- `missing_values_analysis.png` - Missing value visualizations
- `distributions.png` - Distribution plots
- `correlation_heatmap.png` - Correlation matrix

## Quality Metrics
- **Completeness**: Percentage of non-missing values
- **Uniqueness**: Percentage of non-duplicate rows
- **Overall Score**: Weighted combination of metrics

## Key Findings
Data quality issues identified:
- Missing values in specific columns
- Potential outliers in numeric fields
- Duplicate records (if any)
- Data type inconsistencies

---

**Author:** Sherif Elashmawy  
**Date:** January 2026