# Exercise 10: Polar Sportswatch JSON Data Analysis

## Objective
Analyze Polar sportswatch training session data stored in JSON format, extract insights, and create visualizations.

## Data Source
- **Format**: JSON files from Polar sports watch
- **Sessions**: 43 training sessions
- **Date Range**: January - March 2025
- **Data Types**: Exercise metadata, heart rate samples, GPS routes

## Tasks Completed

### 1. JSON Structure Exploration
✅ Analyzed nested JSON structure  
✅ Identified data fields and types  
✅ Extracted exercise metadata  

### 2. Exercise Overview Analysis
✅ Loaded all 43 training sessions  
✅ Calculated duration, distance, calories statistics  
✅ Analyzed sport types and frequencies  

### 3. Heart Rate Analysis
✅ Extracted heart rate time series data  
✅ Created visualizations for sample exercises  
✅ Calculated average, min, max heart rates  

### 4. GPS Route Analysis
✅ Extracted latitude/longitude coordinates  
✅ Created interactive maps with Folium  
✅ Marked start/end points on routes  

### 5. Comprehensive Dashboard
✅ Duration trends over time  
✅ Distance distribution histogram  
✅ Calories burned per exercise  
✅ Summary statistics panel  

## Requirements
```bash
pip install -r requirements.txt
```

## Usage

### Python Script:
```bash
python polar_analysis.py
```

### Jupyter Notebook:
```bash
jupyter notebook json_helper.ipynb
```

## Outputs
- `heart_rate_analysis.png` - Heart rate trends for sample exercises
- `training_dashboard.png` - Comprehensive analysis dashboard
- `route_map_1.html` to `route_map_5.html` - Interactive GPS route maps
- `training_summary.csv` - Summary statistics export

## Data Structure

### JSON Hierarchy
```
{
  "exercises": [
    {
      "startTime": "2025-01-01T...",
      "duration": "...",
      "distance": "...",
      "calories": "...",
      "sport": "...",
      "samples": {
        "heartRate": [...],
        "recordedRoute": [...]
      }
    }
  ]
}
```

### Key Fields
- **startTime**: Exercise start datetime
- **duration**: Exercise duration in seconds
- **distance**: Distance in meters
- **calories**: Calories burned
- **sport**: Type of sport/exercise
- **heartRate**: Time series of heart rate samples
- **recordedRoute**: GPS coordinates (latitude, longitude)

## Technology Stack
- **pandas**: Data manipulation
- **matplotlib**: Static visualizations
- **folium**: Interactive maps
- **jupyter**: Notebook interface

## Key Insights
- Total training hours and distance covered
- Heart rate patterns during exercises
- Route visualization for outdoor activities
- Calorie expenditure trends

---

**Author:** Sherif Elashmawy  
**Date:** January 2026