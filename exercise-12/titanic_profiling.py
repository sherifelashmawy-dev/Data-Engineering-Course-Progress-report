"""
Exercise 12: Titanic Data Profiling
Author: Sherif Elashmawy
Date: January 2026

This script performs comprehensive data profiling on the Titanic dataset:
- Statistical summary
- Survival rate analysis
- Demographic analysis
- Fare and class analysis
- Visual profiling with charts
- Data quality issues identification
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

# Data file
DATA_FILE = 'titanic.csv'


def load_data():
    """
    Load the Titanic dataset.
    
    Returns:
        pd.DataFrame: Loaded data
    """
    print("\n" + "="*60)
    print("Loading Titanic Dataset")
    print("="*60)
    
    df = pd.read_csv(DATA_FILE)
    
    print(f"\n✓ Data loaded successfully")
    print(f"✓ Rows: {len(df):,}")
    print(f"✓ Columns: {len(df.columns)}")
    print()
    
    return df


def identify_data_quality_issues(df):
    """
    Identify data quality issues in the dataset.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Data Quality Issues Detection")
    print("="*60)
    
    issues_found = []
    
    # Check Survived column
    if 'Survived' in df.columns:
        unique_survived = df['Survived'].unique()
        print(f"\nSurvived column unique values: {sorted(unique_survived)}")
        if len(unique_survived) > 2:
            issues_found.append(f"Survived column has {len(unique_survived)} unique values instead of 2")
    
    # Check Sex column
    if 'Sex' in df.columns:
        unique_sex = df['Sex'].value_counts()
        print(f"\nSex column distribution:")
        print(unique_sex.to_string())
        expected_values = {'male', 'female'}
        unexpected = set(unique_sex.index) - expected_values
        if unexpected:
            issues_found.append(f"Sex column has unexpected values: {unexpected}")
    
    # Check Pclass column
    if 'Pclass' in df.columns:
        unique_pclass = df['Pclass'].unique()
        print(f"\nPclass column unique values: {sorted(unique_pclass)}")
        expected_classes = {1.0, 2.0, 3.0}
        unexpected_classes = set(unique_pclass) - expected_classes
        if unexpected_classes:
            issues_found.append(f"Pclass has unexpected values: {unexpected_classes}")
    
    # Check for negative or extreme fares
    if 'Fare' in df.columns:
        negative_fares = (df['Fare'] < 0).sum()
        extreme_fares = (df['Fare'] > 1000).sum()
        if negative_fares > 0:
            issues_found.append(f"Found {negative_fares} negative fare values")
        if extreme_fares > 0:
            issues_found.append(f"Found {extreme_fares} extremely high fares (>£1000)")
    
    if issues_found:
        print(f"\n⚠ Data Quality Issues Found:")
        for i, issue in enumerate(issues_found, 1):
            print(f"  {i}. {issue}")
    else:
        print(f"\n✓ No major data quality issues detected")
    
    print()
    return issues_found


def basic_profiling(df):
    """
    Display basic dataset profiling.
    
    Args:
        df: DataFrame to profile
    """
    print("="*60)
    print("Basic Dataset Profile")
    print("="*60)
    
    print(f"\nDataset Shape: {df.shape}")
    print(f"Rows: {df.shape[0]:,}")
    print(f"Columns: {df.shape[1]}")
    
    print(f"\n\nColumn Information:")
    print(df.dtypes.to_string())
    
    print(f"\n\nFirst 10 Rows:")
    print(df.head(10).to_string())
    
    print(f"\n\nBasic Statistics:")
    print(df.describe(include='all').to_string())
    
    print(f"\n\nMissing Values:")
    missing = df.isnull().sum()
    missing_pct = (missing / len(df)) * 100
    missing_df = pd.DataFrame({
        'Missing_Count': missing,
        'Missing_Percentage': missing_pct
    })
    print(missing_df[missing_df['Missing_Count'] > 0].to_string())
    
    print()


def survival_analysis(df):
    """
    Analyze survival rates.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Survival Rate Analysis")
    print("="*60)
    
    # Check unique survival values
    print(f"\nUnique Survived values: {sorted(df['Survived'].unique())}")
    
    # Overall survival rate (considering only 0 and 1)
    valid_survival = df[df['Survived'].isin([0, 1])]
    total_passengers = len(valid_survival)
    survived = valid_survival['Survived'].sum()
    survival_rate = (survived / total_passengers) * 100 if total_passengers > 0 else 0
    
    print(f"\nOverall Statistics (valid data only):")
    print(f"  Total Passengers: {total_passengers:,}")
    print(f"  Survived: {int(survived)} ({survival_rate:.2f}%)")
    print(f"  Died: {total_passengers - int(survived)} ({100 - survival_rate:.2f}%)")
    print(f"  Invalid/Missing: {len(df) - total_passengers}")
    
    # Survival by gender (for valid data)
    print(f"\n\nSurvival by Gender (valid genders only):")
    valid_df = df[df['Sex'].isin(['male', 'female']) & df['Survived'].isin([0, 1])]
    if len(valid_df) > 0:
        gender_survival = valid_df.groupby('Sex')['Survived'].agg(['sum', 'count', 'mean'])
        gender_survival.columns = ['Survived', 'Total', 'Survival_Rate']
        gender_survival['Survival_Rate'] = gender_survival['Survival_Rate'] * 100
        print(gender_survival.to_string())
    
    # Survival by class (for valid classes 1, 2, 3)
    print(f"\n\nSurvival by Passenger Class (classes 1, 2, 3 only):")
    valid_class_df = df[df['Pclass'].isin([1.0, 2.0, 3.0]) & df['Survived'].isin([0, 1])]
    if len(valid_class_df) > 0:
        class_survival = valid_class_df.groupby('Pclass')['Survived'].agg(['sum', 'count', 'mean'])
        class_survival.columns = ['Survived', 'Total', 'Survival_Rate']
        class_survival['Survival_Rate'] = class_survival['Survival_Rate'] * 100
        print(class_survival.to_string())
    
    # Survival by age group
    if 'Age' in df.columns:
        valid_age_df = valid_df.copy()
        valid_age_df['Age_Group'] = pd.cut(valid_age_df['Age'], bins=[0, 12, 18, 35, 60, 100], 
                                  labels=['Child', 'Teen', 'Adult', 'Middle Age', 'Senior'])
        print(f"\n\nSurvival by Age Group:")
        age_survival = valid_age_df.groupby('Age_Group')['Survived'].agg(['sum', 'count', 'mean'])
        age_survival.columns = ['Survived', 'Total', 'Survival_Rate']
        age_survival['Survival_Rate'] = age_survival['Survival_Rate'] * 100
        print(age_survival.to_string())
    
    print()


def demographic_analysis(df):
    """
    Analyze passenger demographics.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Demographic Analysis")
    print("="*60)
    
    # Gender distribution
    print(f"\nGender Distribution:")
    print(df['Sex'].value_counts().to_string())
    
    # Class distribution
    print(f"\n\nPassenger Class Distribution:")
    print(df['Pclass'].value_counts().sort_index().to_string())
    
    # Age statistics
    if 'Age' in df.columns:
        print(f"\n\nAge Statistics:")
        age_stats = df['Age'].describe()
        print(age_stats.to_string())
        print(f"\nMissing Ages: {df['Age'].isnull().sum()} ({(df['Age'].isnull().sum()/len(df)*100):.2f}%)")
    
    # Family size analysis
    if 'Siblings/Spouses Aboard' in df.columns and 'Parents/Children Aboard' in df.columns:
        df['Family_Size'] = df['Siblings/Spouses Aboard'] + df['Parents/Children Aboard']
        print(f"\n\nFamily Size Statistics:")
        print(f"  Average Family Size: {df['Family_Size'].mean():.2f}")
        print(f"  Traveling Alone: {(df['Family_Size'] == 0).sum()} passengers")
        print(f"  With Family: {(df['Family_Size'] > 0).sum()} passengers")
    
    print()


def fare_analysis(df):
    """
    Analyze fare distribution.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Fare Analysis")
    print("="*60)
    
    # Overall fare statistics
    print(f"\nOverall Fare Statistics:")
    print(df['Fare'].describe().to_string())
    
    # Fare by class (valid classes only)
    print(f"\n\nAverage Fare by Class:")
    fare_by_class = df.groupby('Pclass')['Fare'].agg(['mean', 'median', 'min', 'max'])
    print(fare_by_class.to_string())
    
    # Fare by survival (handle any number of unique values)
    print(f"\n\nAverage Fare by Survival Status:")
    fare_by_survival = df.groupby('Survived')['Fare'].agg(['mean', 'median', 'count'])
    print(fare_by_survival.to_string())
    
    print()


def create_survival_visualizations(df):
    """
    Create survival rate visualizations.
    
    Args:
        df: DataFrame to analyze
    """
    print("="*60)
    print("Creating Survival Visualizations")
    print("="*60)
    
    # Filter to valid data
    valid_df = df[df['Survived'].isin([0, 1]) & 
                  df['Sex'].isin(['male', 'female']) & 
                  df['Pclass'].isin([1.0, 2.0, 3.0])]
    
    if len(valid_df) == 0:
        print("\n⚠ Not enough valid data for visualizations")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Titanic Survival Analysis (Valid Data Only)', fontsize=16, fontweight='bold')
    
    # Plot 1: Overall survival
    ax1 = axes[0, 0]
    survival_counts = valid_df['Survived'].value_counts()
    colors = ['#E74C3C', '#2ECC71']
    ax1.pie(survival_counts, labels=['Died', 'Survived'], autopct='%1.1f%%', 
            colors=colors, startangle=90)
    ax1.set_title('Overall Survival Rate', fontweight='bold')
    
    # Plot 2: Survival by gender
    ax2 = axes[0, 1]
    gender_survival = valid_df.groupby(['Sex', 'Survived']).size().unstack()
    gender_survival.plot(kind='bar', ax=ax2, color=['#E74C3C', '#2ECC71'])
    ax2.set_title('Survival by Gender', fontweight='bold')
    ax2.set_xlabel('Gender')
    ax2.set_ylabel('Count')
    ax2.legend(['Died', 'Survived'])
    ax2.set_xticklabels(ax2.get_xticklabels(), rotation=0)
    
    # Plot 3: Survival by class
    ax3 = axes[1, 0]
    class_survival = valid_df.groupby(['Pclass', 'Survived']).size().unstack()
    class_survival.plot(kind='bar', ax=ax3, color=['#E74C3C', '#2ECC71'])
    ax3.set_title('Survival by Passenger Class', fontweight='bold')
    ax3.set_xlabel('Class')
    ax3.set_ylabel('Count')
    ax3.legend(['Died', 'Survived'])
    ax3.set_xticklabels(['1st Class', '2nd Class', '3rd Class'], rotation=0)
    
    # Plot 4: Age distribution
    ax4 = axes[1, 1]
    survived = valid_df[valid_df['Survived'] == 1]['Age'].dropna()
    died = valid_df[valid_df['Survived'] == 0]['Age'].dropna()
    ax4.hist([died, survived], bins=20, label=['Died', 'Survived'], 
             color=['#E74C3C', '#2ECC71'], alpha=0.7)
    ax4.set_title('Age Distribution by Survival', fontweight='bold')
    ax4.set_xlabel('Age')
    ax4.set_ylabel('Count')
    ax4.legend()
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('survival_analysis.png', dpi=300, bbox_inches='tight')
    print("\n✓ Survival visualizations saved to: survival_analysis.png")
    plt.close()


def create_demographic_visualizations(df):
    """
    Create demographic visualizations.
    
    Args:
        df: DataFrame to analyze
    """
    print("\n" + "="*60)
    print("Creating Demographic Visualizations")
    print("="*60)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Titanic Passenger Demographics', fontsize=16, fontweight='bold')
    
    # Plot 1: Class distribution (valid classes only)
    ax1 = axes[0, 0]
    valid_classes = df[df['Pclass'].isin([1.0, 2.0, 3.0])]
    class_counts = valid_classes['Pclass'].value_counts().sort_index()
    ax1.bar(['1st Class', '2nd Class', '3rd Class'], class_counts.values, 
            color=['#3498DB', '#9B59B6', '#E67E22'])
    ax1.set_title('Passenger Class Distribution', fontweight='bold')
    ax1.set_ylabel('Count')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Plot 2: Age distribution
    ax2 = axes[0, 1]
    ax2.hist(df['Age'].dropna(), bins=30, color='#3498DB', alpha=0.7, edgecolor='black')
    ax2.set_title('Age Distribution', fontweight='bold')
    ax2.set_xlabel('Age')
    ax2.set_ylabel('Count')
    ax2.axvline(df['Age'].mean(), color='red', linestyle='--', 
                linewidth=2, label=f'Mean: {df["Age"].mean():.1f}')
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Plot 3: Fare distribution (reasonable fares only)
    ax3 = axes[1, 0]
    reasonable_fares = df[(df['Fare'] >= 0) & (df['Fare'] <= 300)]['Fare']
    ax3.hist(reasonable_fares, bins=50, color='#2ECC71', alpha=0.7, edgecolor='black')
    ax3.set_title('Fare Distribution (£0-£300)', fontweight='bold')
    ax3.set_xlabel('Fare (£)')
    ax3.set_ylabel('Count')
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Plot 4: Family size distribution
    ax4 = axes[1, 1]
    if 'Family_Size' in df.columns:
        family_counts = df['Family_Size'].value_counts().sort_index()
        ax4.bar(family_counts.index, family_counts.values, color='#E74C3C', alpha=0.7)
        ax4.set_title('Family Size Distribution', fontweight='bold')
        ax4.set_xlabel('Family Size')
        ax4.set_ylabel('Count')
        ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('demographics.png', dpi=300, bbox_inches='tight')
    print("\n✓ Demographic visualizations saved to: demographics.png")
    plt.close()


def create_fare_analysis_plot(df):
    """
    Create fare analysis visualizations.
    
    Args:
        df: DataFrame to analyze
    """
    print("\n" + "="*60)
    print("Creating Fare Analysis Visualization")
    print("="*60)
    
    # Filter to valid data
    valid_df = df[df['Pclass'].isin([1.0, 2.0, 3.0]) & 
                  (df['Fare'] >= 0) & (df['Fare'] <= 300)]
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Fare Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Fare by class
    ax1 = axes[0]
    valid_df.boxplot(column='Fare', by='Pclass', ax=ax1)
    ax1.set_title('Fare Distribution by Class', fontweight='bold')
    ax1.set_xlabel('Passenger Class')
    ax1.set_ylabel('Fare (£)')
    plt.sca(ax1)
    plt.xticks([1, 2, 3], ['1st Class', '2nd Class', '3rd Class'])
    
    # Plot 2: Average fare by class
    ax2 = axes[1]
    avg_fares = valid_df.groupby('Pclass')['Fare'].mean()
    ax2.bar(['1st Class', '2nd Class', '3rd Class'], avg_fares.values, 
            color=['#3498DB', '#9B59B6', '#E67E22'], alpha=0.7)
    ax2.set_title('Average Fare by Class', fontweight='bold')
    ax2.set_xlabel('Passenger Class')
    ax2.set_ylabel('Average Fare (£)')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for i, v in enumerate(avg_fares.values):
        ax2.text(i, v + 2, f'£{v:.2f}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('fare_analysis.png', dpi=300, bbox_inches='tight')
    print("\n✓ Fare analysis saved to: fare_analysis.png")
    plt.close()


def generate_profile_report(df, data_quality_issues):
    """
    Generate a comprehensive profiling report.
    
    Args:
        df: DataFrame to profile
        data_quality_issues: List of data quality issues
    """
    print("="*60)
    print("Generating Profile Report")
    print("="*60)
    
    report_lines = []
    report_lines.append("="*60)
    report_lines.append("TITANIC DATASET PROFILE REPORT")
    report_lines.append("="*60)
    
    # Dataset overview
    report_lines.append("\n\nDATASET OVERVIEW")
    report_lines.append("-" * 60)
    report_lines.append(f"Total Passengers: {len(df):,}")
    report_lines.append(f"Total Features: {len(df.columns)}")
    
    # Data quality
    report_lines.append("\n\nDATA QUALITY ASSESSMENT")
    report_lines.append("-" * 60)
    if data_quality_issues:
        report_lines.append(f"Issues Found: {len(data_quality_issues)}")
        for i, issue in enumerate(data_quality_issues, 1):
            report_lines.append(f"  {i}. {issue}")
    else:
        report_lines.append("✓ No major data quality issues detected")
    
    # Survival statistics (valid data only)
    valid_df = df[df['Survived'].isin([0, 1])]
    report_lines.append("\n\nSURVIVAL STATISTICS (Valid Data)")
    report_lines.append("-" * 60)
    survived = valid_df['Survived'].sum()
    survival_rate = (survived / len(valid_df)) * 100 if len(valid_df) > 0 else 0
    report_lines.append(f"Total Valid Records: {len(valid_df)}")
    report_lines.append(f"Survived: {int(survived)} ({survival_rate:.2f}%)")
    report_lines.append(f"Died: {len(valid_df) - int(survived)} ({100 - survival_rate:.2f}%)")
    
    # Demographics
    valid_gender_df = df[df['Sex'].isin(['male', 'female'])]
    report_lines.append("\n\nDEMOGRAPHICS (Valid Data)")
    report_lines.append("-" * 60)
    report_lines.append(f"Male: {(valid_gender_df['Sex'] == 'male').sum()}")
    report_lines.append(f"Female: {(valid_gender_df['Sex'] == 'female').sum()}")
    report_lines.append(f"\nAverage Age: {df['Age'].mean():.1f} years")
    report_lines.append(f"Age Range: {df['Age'].min():.0f} - {df['Age'].max():.0f} years")
    
    # Class distribution (valid classes)
    valid_classes = df[df['Pclass'].isin([1.0, 2.0, 3.0])]
    report_lines.append("\n\nCLASS DISTRIBUTION (Valid Classes)")
    report_lines.append("-" * 60)
    for pclass in sorted(valid_classes['Pclass'].unique()):
        count = (valid_classes['Pclass'] == pclass).sum()
        pct = (count / len(valid_classes)) * 100
        report_lines.append(f"Class {int(pclass)}: {count} ({pct:.1f}%)")
    
    # Key insights
    report_lines.append("\n\nKEY INSIGHTS (Valid Data)")
    report_lines.append("-" * 60)
    
    valid_analysis_df = df[df['Survived'].isin([0, 1]) & df['Sex'].isin(['male', 'female']) & df['Pclass'].isin([1.0, 2.0, 3.0])]
    if len(valid_analysis_df) > 0:
        female_survival = valid_analysis_df[valid_analysis_df['Sex'] == 'female']['Survived'].mean() * 100
        male_survival = valid_analysis_df[valid_analysis_df['Sex'] == 'male']['Survived'].mean() * 100
        report_lines.append(f"- Female survival rate: {female_survival:.1f}%")
        report_lines.append(f"- Male survival rate: {male_survival:.1f}%")
        
        class1_survival = valid_analysis_df[valid_analysis_df['Pclass'] == 1.0]['Survived'].mean() * 100
        class3_survival = valid_analysis_df[valid_analysis_df['Pclass'] == 3.0]['Survived'].mean() * 100
        report_lines.append(f"- 1st class survival rate: {class1_survival:.1f}%")
        report_lines.append(f"- 3rd class survival rate: {class3_survival:.1f}%")
    
    reasonable_fares = df[(df['Fare'] >= 0) & (df['Fare'] <= 1000)]
    report_lines.append(f"\n- Average fare (reasonable): £{reasonable_fares['Fare'].mean():.2f}")
    report_lines.append(f"- Median fare: £{reasonable_fares['Fare'].median():.2f}")
    
    report_lines.append("\n" + "="*60)
    
    # Write to file
    report_text = "\n".join(report_lines)
    
    with open('titanic_profile_report.txt', 'w') as f:
        f.write(report_text)
    
    print("\n✓ Profile report saved to: titanic_profile_report.txt")
    print(f"\n{report_text}")
    print()


def export_summary_statistics(df):
    """
    Export summary statistics to CSV.
    
    Args:
        df: DataFrame to export
    """
    # Create summary statistics
    summary = df.describe(include='all').T
    summary.to_csv('titanic_statistics.csv')
    print("✓ Summary statistics exported to: titanic_statistics.csv")


def main():
    """
    Main execution function.
    """
    print("\n" + "="*60)
    print("Exercise 12: Titanic Data Profiling")
    print("="*60)
    
    # Load data
    df = load_data()
    
    # Identify data quality issues
    data_quality_issues = identify_data_quality_issues(df)
    
    # Basic profiling
    basic_profiling(df)
    
    # Survival analysis
    survival_analysis(df)
    
    # Demographic analysis
    demographic_analysis(df)
    
    # Fare analysis
    fare_analysis(df)
    
    # Create visualizations
    create_survival_visualizations(df)
    create_demographic_visualizations(df)
    create_fare_analysis_plot(df)
    
    # Generate report
    generate_profile_report(df, data_quality_issues)
    
    # Export statistics
    export_summary_statistics(df)
    
    print("\n" + "="*60)
    print("✓ All profiling completed successfully!")
    print("="*60 + "\n")
    
    print("Outputs:")
    print("  - titanic_profile_report.txt")
    print("  - survival_analysis.png")
    print("  - demographics.png")
    print("  - fare_analysis.png")
    print("  - titanic_statistics.csv")
    print()


if __name__ == "__main__":
    main()
