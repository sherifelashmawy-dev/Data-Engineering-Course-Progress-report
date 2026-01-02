# Exercise 01: Global Temperature Data Analysis

## Objective
Analyze global temperature data from GitHub, performing data processing, transformation, comparison, visualization, and future estimation tasks.

## Data Source
- **Dataset:** Global temperature anomalies
- **URL:** https://raw.githubusercontent.com/datasets/global-temp/master/data/annual.csv
- **Sources:** GCAG (NOAA) and GISTEMP (NASA)

## Tasks Completed

### a) Data Storage
- ✅ Load data from GitHub
- ✅ Save to local CSV file (`global_temperature.csv`)
- ✅ Save to SQLite database (`temperature_data.db`)

### b) Time Format Conversion
- ✅ Convert year to Pandas datetime format
- ✅ Calculate Unix epoch timestamps

### c) Data Reshaping
- ✅ Pivot data so sources become column headers
- ✅ Transform from long format to wide format

### d) Source Comparison
- ✅ Compare GCAG vs GISTEMP observations
- ✅ Calculate statistical differences
- ✅ Compute correlation between sources

### e) Visualization
- ✅ Plot temperature trends for each source
- ✅ Save high-resolution plot

### f) Future Estimation
- ✅ Use linear, quadratic, and cubic interpolation
- ✅ Predict temperatures for next 10 years
- ✅ Visualize predictions vs historical data

## Requirements
```bash
pandas==2.1.4
numpy==1.26.2
matplotlib==3.8.2
scipy==1.11.4
```

## Installation
```bash
pip install -r requirements.txt
```

## Usage
```bash
python global_temperature.py
```

## Output Files
- `global_temperature.csv` - Raw data in CSV format
- `temperature_data.db` - SQLite database
- `temperature_trends.png` - Visualization of historical trends
- `future_estimation.png` - Future temperature predictions

## Key Findings
- Both data sources (GCAG and GISTEMP) show consistent warming trends
- Strong correlation between the two sources (typically > 0.99)
- Interpolation methods show continued warming trend
- Different interpolation methods provide varying future estimates

## Technical Approach
1. **Data Loading:** Used pandas to read CSV from GitHub
2. **Database:** SQLite for structured storage
3. **Time Conversion:** Pandas datetime and NumPy for epoch calculation
4. **Reshaping:** Pivot tables to transform data structure
5. **Statistical Analysis:** Correlation and difference metrics
6. **Visualization:** Matplotlib for professional plots
7. **Interpolation:** SciPy's interp1d with multiple methods

---

**Author:** Sherif Elashmawy  
**Date:** January 2026