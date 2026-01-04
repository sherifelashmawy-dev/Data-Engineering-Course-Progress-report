"""
Electricity Price Dashboard
Streamlit app to visualize electricity prices from Kafka consumer data.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import pytz
import os

# Configuration
CSV_PATH = 'data/electricity_prices.csv'

# Page config
st.set_page_config(
    page_title="Electricity Price Monitor",
    page_icon="⚡",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .big-font {
        font-size:30px !important;
        font-weight: bold;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=10)
def load_data():
    """Load electricity price data from CSV."""
    if not os.path.exists(CSV_PATH):
        return None
    
    df = pd.read_csv(CSV_PATH)
    df['startDate'] = pd.to_datetime(df['startDate'], utc=True)
    df['endDate'] = pd.to_datetime(df['endDate'], utc=True)
    df['received_timestamp'] = pd.to_datetime(df['received_timestamp'])
    
    return df

def main():
    st.title("⚡ Real-Time Electricity Price Monitor")
    st.markdown("---")
    
    # Load data
    df = load_data()
    
    if df is None or len(df) == 0:
        st.warning("⚠️ No data available yet. Make sure the consumer is running!")
        st.info("Start the consumer with: `python consumer.py`")
        return
    
    # Sidebar controls
    st.sidebar.header("📊 Dashboard Controls")
    
    # Auto-refresh
    auto_refresh = st.sidebar.checkbox("Auto-refresh (every 10s)", value=True)
    if auto_refresh:
        import time
        time.sleep(10)
        st.rerun()
    
    # Time range filter
    hours_back = st.sidebar.slider("Hours to display", 1, 48, 24)
    cutoff_time = pd.Timestamp.now(tz='UTC') - timedelta(hours=hours_back)
    df_filtered = df[df['startDate'] >= cutoff_time].copy()
    
    # Sort by start date
    df_filtered = df_filtered.sort_values('startDate')
    
    if len(df_filtered) == 0:
        st.warning(f"⚠️ No data in the last {hours_back} hours. Try increasing the time range.")
        return
    
    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        current_price = df_filtered.iloc[-1]['price'] if len(df_filtered) > 0 else 0
        st.metric("Current Price", f"{current_price:.3f} ¢/kWh", 
                 delta=f"{current_price - df_filtered['price'].mean():.3f}")
    
    with col2:
        avg_price = df_filtered['price'].mean()
        st.metric("Average Price", f"{avg_price:.3f} ¢/kWh")
    
    with col3:
        min_price = df_filtered['price'].min()
        min_time = df_filtered[df_filtered['price'] == min_price]['startDate'].iloc[0]
        st.metric("Lowest Price", f"{min_price:.3f} ¢/kWh",
                 delta=min_time.strftime("%H:%M"))
    
    with col4:
        max_price = df_filtered['price'].max()
        max_time = df_filtered[df_filtered['price'] == max_price]['startDate'].iloc[0]
        st.metric("Highest Price", f"{max_price:.3f} ¢/kWh",
                 delta=max_time.strftime("%H:%M"))
    
    st.markdown("---")
    
    # Main price chart
    st.subheader("📈 Price Trend")
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_filtered['startDate'],
        y=df_filtered['price'],
        mode='lines+markers',
        name='Electricity Price',
        line=dict(color='#FF6B6B', width=2),
        marker=dict(size=4),
        hovertemplate='<b>Time:</b> %{x}<br><b>Price:</b> %{y:.3f} ¢/kWh<extra></extra>'
    ))
    
    # Add average line
    fig.add_hline(y=avg_price, line_dash="dash", line_color="green",
                  annotation_text=f"Average: {avg_price:.3f} ¢/kWh")
    
    fig.update_layout(
        height=500,
        xaxis_title="Time",
        yaxis_title="Price (cents/kWh)",
        hovermode='x unified',
        template="plotly_white"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Distribution and hourly analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Price Distribution")
        fig_hist = px.histogram(
            df_filtered,
            x='price',
            nbins=30,
            color_discrete_sequence=['#4ECDC4']
        )
        fig_hist.update_layout(
            xaxis_title="Price (cents/kWh)",
            yaxis_title="Frequency",
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        st.subheader("🕐 Hourly Average Prices")
        df_filtered['hour'] = df_filtered['startDate'].dt.hour
        hourly_avg = df_filtered.groupby('hour')['price'].mean().reset_index()
        
        fig_hourly = px.bar(
            hourly_avg,
            x='hour',
            y='price',
            color='price',
            color_continuous_scale='RdYlGn_r'
        )
        fig_hourly.update_layout(
            xaxis_title="Hour of Day",
            yaxis_title="Average Price (cents/kWh)",
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    # Data table
    st.markdown("---")
    st.subheader("📋 Recent Price Data")
    
    # Show last 20 records
    display_df = df_filtered.tail(20)[['startDate', 'price']].copy()
    display_df['startDate'] = display_df['startDate'].dt.strftime('%Y-%m-%d %H:%M')
    display_df = display_df.rename(columns={
        'startDate': 'Time',
        'price': 'Price (¢/kWh)'
    })
    display_df = display_df.sort_values('Time', ascending=False)
    
    st.dataframe(display_df, use_container_width=True, height=400)
    
    # Statistics
    st.markdown("---")
    st.subheader("📊 Statistics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Price Range**")
        st.write(f"Min: {df_filtered['price'].min():.3f} ¢/kWh")
        st.write(f"Max: {df_filtered['price'].max():.3f} ¢/kWh")
        st.write(f"Range: {df_filtered['price'].max() - df_filtered['price'].min():.3f} ¢/kWh")
    
    with col2:
        st.write("**Central Tendency**")
        st.write(f"Mean: {df_filtered['price'].mean():.3f} ¢/kWh")
        st.write(f"Median: {df_filtered['price'].median():.3f} ¢/kWh")
        st.write(f"Std Dev: {df_filtered['price'].std():.3f}")
    
    with col3:
        st.write("**Data Info**")
        st.write(f"Total Records: {len(df):,}")
        st.write(f"Displayed: {len(df_filtered):,}")
        st.write(f"Last Update: {df['received_timestamp'].max().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Footer
    st.markdown("---")
    st.caption("⚡ Real-time electricity price monitoring powered by Kafka and Streamlit")


if __name__ == "__main__":
    main()
