"""
Streamlit Population Visualization
Displays global population trends from 1952-2007
"""

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Read data
df_visu = pd.read_csv('population_country_columns.csv')

# Page configuration
st.title('Global Population Trends (1952-2007)')
st.markdown('**Data Source:** World Bank Population Data')

# Section 1: Interactive Line Chart
st.subheader('📈 Population Trends - Line Chart')
st.markdown('Select countries to compare population changes over time:')

# Country selector for line chart
columns_line = st.multiselect(
    'Select Countries for Line Chart',
    (df_visu.drop(columns=['year'])).columns,
    default=['China', 'India', 'United States'] if all(c in df_visu.columns for c in ['China', 'India', 'United States']) else [],
    key='line_selector'
)

# Plot line chart
if columns_line:
    st.line_chart(df_visu, x='year', y=columns_line, y_label='Population', x_label='Year')
else:
    st.info('👆 Select one or more countries to display the line chart')

st.markdown('---')

# Section 2: Stacked Bar Chart
st.subheader('📊 Population Distribution - Stacked Bar Chart')
st.markdown('Select countries to view their combined population distribution:')

# Country selector for bar chart
columns_bar = st.multiselect(
    'Select Countries for Stacked Bar Chart',
    (df_visu.drop(columns=['year'])).columns,
    default=['China', 'India', 'United States'] if all(c in df_visu.columns for c in ['China', 'India', 'United States']) else [],
    key='bar_selector'
)

# Plot stacked bar chart
if columns_bar:
    fig, ax = plt.subplots(figsize=(12, 6))
    bottom_line = 0
    
    for country_name in columns_bar:
        plt.bar(
            df_visu['year'],
            df_visu[country_name] / 1000000,
            bottom=bottom_line,
            width=4,
            label=country_name
        )
        bottom_line = bottom_line + (df_visu[country_name].values) / 1000000
    
    plt.title('Global Population Stacked View', fontsize=14, fontweight='bold')
    plt.ylabel('Population (millions)', fontsize=12)
    plt.xlabel('Year', fontsize=12)
    plt.legend(loc='upper left', fontsize=10)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    st.pyplot(fig)
else:
    st.info('👆 Select one or more countries to display the stacked bar chart')

# Footer
st.markdown('---')
st.caption('Built with Streamlit | Deployed on CSC cPouta Cloud')
