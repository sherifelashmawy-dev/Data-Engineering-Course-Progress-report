"""
Exercise 10: Polar Sportswatch JSON Data Analysis
Author: Sherif Elashmawy
Date: January 2026

This script analyzes Polar sportswatch training session data:
- Reads multiple JSON files
- Extracts exercise information, heart rate, and GPS routes
- Creates visualizations and maps
- Analyzes training patterns
"""

import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import folium
from datetime import datetime

# Directory containing JSON files
DATA_DIR = "Polar_for_Exercise"


def print_json_structure(obj, indent=0):
    """
    Print the structure of a JSON object.
    
    Args:
        obj: JSON object to analyze
        indent: Indentation level
    """
    prefix = " " * indent
    if isinstance(obj, dict):
        for k, v in obj.items():
            print(f"{prefix}{k}: {type(v).__name__}")
            print_json_structure(v, indent + 4)
    elif isinstance(obj, list):
        print(f"{prefix}List[{len(obj)}]")
        if obj:
            print_json_structure(obj[0], indent + 4)


def explore_json_structure():
    """
    Explore the structure of a single JSON file.
    """
    print("\n" + "="*60)
    print("Exploring JSON Structure")
    print("="*60)
    
    # Get first JSON file
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json')])
    if not json_files:
        print("No JSON files found!")
        return
    
    first_file = os.path.join(DATA_DIR, json_files[0])
    print(f"\nAnalyzing: {json_files[0]}\n")
    
    with open(first_file, 'r') as file:
        data = json.load(file)
    
    print_json_structure(data)
    print()


def load_all_exercises():
    """
    Load exercise overview data from all JSON files.
    
    Returns:
        pd.DataFrame: Combined exercise data
    """
    print("="*60)
    print("Loading All Exercise Data")
    print("="*60)
    
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json')])
    print(f"\nFound {len(json_files)} JSON files")
    
    all_exercises = []
    
    for json_file in json_files:
        file_path = os.path.join(DATA_DIR, json_file)
        
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # Extract exercise overview
            if 'exercises' in data and len(data['exercises']) > 0:
                exercise_data = data['exercises'][0]
                
                # Add filename for reference
                exercise_data['filename'] = json_file
                all_exercises.append(exercise_data)
        
        except Exception as e:
            print(f"Error loading {json_file}: {e}")
    
    df = pd.json_normalize(all_exercises)
    
    print(f"\n✓ Loaded {len(df)} exercises")
    print(f"✓ Columns: {len(df.columns)}")
    print()
    
    return df


def analyze_exercise_overview(df):
    """
    Analyze exercise overview data.
    
    Args:
        df: DataFrame with exercise data
    """
    print("="*60)
    print("Exercise Overview Analysis")
    print("="*60)
    
    print(f"\nTotal Exercises: {len(df)}")
    
    # Convert startTime to datetime
    if 'startTime' in df.columns:
        df['startTime'] = pd.to_datetime(df['startTime'])
        print(f"Date Range: {df['startTime'].min().date()} to {df['startTime'].max().date()}")
    
    # Convert duration to numeric (handle ISO format or direct seconds)
    if 'duration' in df.columns:
        df['duration_seconds'] = pd.to_timedelta(df['duration']).dt.total_seconds()
        
        print(f"\nDuration Statistics (minutes):")
        print(f"  Total: {df['duration_seconds'].sum() / 60:.1f} minutes")
        print(f"  Average: {df['duration_seconds'].mean() / 60:.1f} minutes")
        print(f"  Longest: {df['duration_seconds'].max() / 60:.1f} minutes")
        print(f"  Shortest: {df['duration_seconds'].min() / 60:.1f} minutes")
    
    if 'distance' in df.columns:
        df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
        print(f"\nDistance Statistics (km):")
        print(f"  Total: {df['distance'].sum() / 1000:.1f} km")
        print(f"  Average: {df['distance'].mean() / 1000:.1f} km")
        print(f"  Longest: {df['distance'].max() / 1000:.1f} km")
    
    # Handle calories (might be 'calories' or 'kiloCalories')
    calories_col = None
    if 'calories' in df.columns:
        calories_col = 'calories'
    elif 'kiloCalories' in df.columns:
        calories_col = 'kiloCalories'
    
    if calories_col:
        df['calories_numeric'] = pd.to_numeric(df[calories_col], errors='coerce')
        print(f"\nCalories Statistics:")
        print(f"  Total: {df['calories_numeric'].sum():.0f} kcal")
        print(f"  Average: {df['calories_numeric'].mean():.0f} kcal")
    
    if 'sport' in df.columns:
        print(f"\nSport Types:")
        print(df['sport'].value_counts().to_string())
    
    print()
    
    return df


def extract_heart_rate_data(json_file):
    """
    Extract heart rate data from a single JSON file.
    
    Args:
        json_file: Path to JSON file
        
    Returns:
        pd.DataFrame: Heart rate data
    """
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    if 'exercises' in data and len(data['exercises']) > 0:
        if 'samples' in data['exercises'][0]:
            if 'heartRate' in data['exercises'][0]['samples']:
                hr_samples = data['exercises'][0]['samples']['heartRate']
                df_hr = pd.DataFrame(hr_samples)
                return df_hr
    
    return pd.DataFrame()


def extract_route_data(json_file):
    """
    Extract GPS route data from a single JSON file.
    
    Args:
        json_file: Path to JSON file
        
    Returns:
        pd.DataFrame: Route data with latitude and longitude
    """
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    if 'exercises' in data and len(data['exercises']) > 0:
        if 'samples' in data['exercises'][0]:
            if 'recordedRoute' in data['exercises'][0]['samples']:
                route_samples = data['exercises'][0]['samples']['recordedRoute']
                df_route = pd.DataFrame(route_samples)
                return df_route
    
    return pd.DataFrame()


def create_heart_rate_visualization():
    """
    Create heart rate visualization for sample exercises.
    """
    print("="*60)
    print("Creating Heart Rate Visualizations")
    print("="*60)
    
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json')])[:5]
    
    fig, axes = plt.subplots(len(json_files), 1, figsize=(14, 3*len(json_files)))
    if len(json_files) == 1:
        axes = [axes]
    
    for idx, json_file in enumerate(json_files):
        file_path = os.path.join(DATA_DIR, json_file)
        df_hr = extract_heart_rate_data(file_path)
        
        if not df_hr.empty and 'value' in df_hr.columns:
            axes[idx].plot(df_hr.index, df_hr['value'], color='#E74C3C', linewidth=1.5)
            axes[idx].set_title(f'Heart Rate - {json_file[:20]}...', fontsize=10)
            axes[idx].set_xlabel('Sample Index')
            axes[idx].set_ylabel('Heart Rate (bpm)')
            axes[idx].grid(True, alpha=0.3)
            axes[idx].axhline(y=df_hr['value'].mean(), color='blue', linestyle='--', 
                            label=f'Avg: {df_hr["value"].mean():.0f} bpm', alpha=0.7)
            axes[idx].legend()
    
    plt.tight_layout()
    plt.savefig('heart_rate_analysis.png', dpi=300, bbox_inches='tight')
    print("\n✓ Heart rate visualization saved to: heart_rate_analysis.png")
    plt.close()


def create_route_map(json_file, output_name):
    """
    Create an interactive map for a single exercise route.
    
    Args:
        json_file: Path to JSON file
        output_name: Output HTML filename
    """
    df_route = extract_route_data(json_file)
    
    if df_route.empty or 'latitude' not in df_route.columns:
        print(f"No route data found in {json_file}")
        return
    
    # Calculate center
    lat_mean = df_route['latitude'].mean()
    lon_mean = df_route['longitude'].mean()
    
    # Create map
    my_map = folium.Map(location=[lat_mean, lon_mean], zoom_start=14)
    
    # Draw route
    folium.PolyLine(
        df_route[['latitude', 'longitude']].values.tolist(),
        color='blue',
        weight=3,
        opacity=0.8
    ).add_to(my_map)
    
    # Add start and end markers
    folium.Marker(
        [df_route.iloc[0]['latitude'], df_route.iloc[0]['longitude']],
        popup='Start',
        icon=folium.Icon(color='green', icon='play')
    ).add_to(my_map)
    
    folium.Marker(
        [df_route.iloc[-1]['latitude'], df_route.iloc[-1]['longitude']],
        popup='End',
        icon=folium.Icon(color='red', icon='stop')
    ).add_to(my_map)
    
    # Save map
    my_map.save(output_name)
    print(f"✓ Route map saved to: {output_name}")


def create_all_route_maps():
    """
    Create route maps for exercises that have GPS data.
    """
    print("\n" + "="*60)
    print("Creating Route Maps")
    print("="*60 + "\n")
    
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json')])
    
    maps_created = 0
    for idx, json_file in enumerate(json_files[:5]):  # Limit to first 5
        file_path = os.path.join(DATA_DIR, json_file)
        output_name = f'route_map_{idx+1}.html'
        
        try:
            create_route_map(file_path, output_name)
            maps_created += 1
        except Exception as e:
            print(f"Could not create map for {json_file}: {e}")
    
    print(f"\n✓ Created {maps_created} route maps")
    print()


def create_summary_dashboard(df):
    """
    Create a summary dashboard with multiple visualizations.
    
    Args:
        df: DataFrame with exercise data
    """
    print("="*60)
    print("Creating Summary Dashboard")
    print("="*60)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Polar Training Session Analysis Dashboard', fontsize=16, fontweight='bold')
    
    # Plot 1: Duration over time
    if 'startTime' in df.columns and 'duration_seconds' in df.columns:
        ax1 = axes[0, 0]
        df_sorted = df.sort_values('startTime')
        ax1.plot(df_sorted['startTime'], df_sorted['duration_seconds'] / 60, marker='o', linewidth=2, markersize=4)
        ax1.set_title('Exercise Duration Over Time', fontweight='bold')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Duration (minutes)')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Distance distribution
    if 'distance' in df.columns:
        ax2 = axes[0, 1]
        ax2.hist(df['distance'].dropna() / 1000, bins=20, color='#3498DB', alpha=0.7, edgecolor='black')
        ax2.set_title('Distance Distribution', fontweight='bold')
        ax2.set_xlabel('Distance (km)')
        ax2.set_ylabel('Frequency')
        ax2.grid(True, alpha=0.3, axis='y')
    
    # Plot 3: Calories per exercise (if available)
    if 'calories_numeric' in df.columns:
        ax3 = axes[1, 0]
        ax3.bar(range(len(df)), df['calories_numeric'].values, color='#E74C3C', alpha=0.7)
        ax3.set_title('Calories Burned Per Exercise', fontweight='bold')
        ax3.set_xlabel('Exercise Number')
        ax3.set_ylabel('Calories (kcal)')
        ax3.grid(True, alpha=0.3, axis='y')
    else:
        axes[1, 0].text(0.5, 0.5, 'No Calories Data Available', 
                       ha='center', va='center', fontsize=14)
        axes[1, 0].axis('off')
    
    # Plot 4: Summary stats
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Build stats text dynamically
    stats_lines = [
        "Training Summary",
        "",
        f"Total Exercises: {len(df)}",
        ""
    ]
    
    if 'duration_seconds' in df.columns:
        stats_lines.extend([
            "Duration:",
            f"  Total: {df['duration_seconds'].sum() / 3600:.1f} hours",
            f"  Average: {df['duration_seconds'].mean() / 60:.1f} minutes",
            ""
        ])
    
    if 'distance' in df.columns:
        stats_lines.extend([
            "Distance:",
            f"  Total: {df['distance'].sum() / 1000:.1f} km",
            f"  Average: {df['distance'].mean() / 1000:.1f} km",
            ""
        ])
    
    if 'calories_numeric' in df.columns:
        stats_lines.extend([
            "Calories:",
            f"  Total: {df['calories_numeric'].sum():.0f} kcal",
            f"  Average: {df['calories_numeric'].mean():.0f} kcal"
        ])
    
    stats_text = "\n".join(stats_lines)
    
    ax4.text(0.1, 0.5, stats_text, fontsize=12, verticalalignment='center',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig('training_dashboard.png', dpi=300, bbox_inches='tight')
    print("\n✓ Dashboard saved to: training_dashboard.png")
    plt.close()


def export_summary_to_csv(df, filename='training_summary.csv'):
    """
    Export training summary to CSV.
    
    Args:
        df: DataFrame to export
        filename: Output filename
    """
    # Select key columns that exist
    columns_to_export = []
    for col in ['startTime', 'sport', 'duration', 'duration_seconds', 'distance', 'calories', 'calories_numeric', 'kiloCalories']:
        if col in df.columns:
            columns_to_export.append(col)
    
    if columns_to_export:
        df_export = df[columns_to_export].copy()
        df_export.to_csv(filename, index=False)
        print(f"✓ Training summary exported to: {filename}")
    else:
        print("⚠ No columns available to export")


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 10: Polar Sportswatch JSON Data Analysis")
    print("="*60)
    
    # Explore JSON structure
    explore_json_structure()
    
    # Load all exercise data
    df_exercises = load_all_exercises()
    
    # Analyze overview
    df_exercises = analyze_exercise_overview(df_exercises)
    
    # Create visualizations
    create_heart_rate_visualization()
    create_summary_dashboard(df_exercises)
    
    # Create route maps
    create_all_route_maps()
    
    # Export to CSV
    export_summary_to_csv(df_exercises)
    
    print("\n" + "="*60)
    print("✓ All analysis completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print("  - heart_rate_analysis.png")
    print("  - training_dashboard.png")
    print("  - route_map_1.html to route_map_5.html")
    print("  - training_summary.csv")
    print()


if __name__ == "__main__":
    main()
