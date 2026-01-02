# Exercise 05: BigQuery Storage Cost Estimation

## Objective
Estimate BigQuery storage costs over 5 years for a company that collects 5 GB of data daily and keeps all historical data.

## Scenario
- **Data Collection Rate**: 5 GB per day
- **Retention Policy**: Keep all data (no deletion)
- **Time Period**: 5 years
- **Analysis**: Calculate storage costs with BigQuery pricing tiers

## BigQuery Pricing (January 2026)
Source: https://cloud.google.com/bigquery/pricing#storage

- **Active Storage** (0-90 days old): $0.02 per GB per month
- **Long-term Storage** (90+ days old): $0.01 per GB per month  
- **Free Tier**: First 10 GB free

## Calculations

### Data Accumulation
- Daily: 5 GB
- Monthly: ~152 GB (30.44 days avg)
- Yearly: ~1,825 GB (1.78 TB)
- 5 Years: ~9,125 GB (8.9 TB)

### Cost Structure
- First 90 days: All data in active storage
- After 90 days: Data splits between active (last 90 days) and long-term (older)
- Free tier applied to active storage only

## Key Findings
- **Total 5-year cost**: Calculated in analysis
- **Monthly cost (Year 5)**: Stabilizes after 90 days
- **Savings from long-term pricing**: Significant compared to all-active pricing
- **Cost vs. deletion strategy**: Analyzed in alternative scenarios

## Requirements
```bash
pip install -r requirements.txt
```

## Usage
```bash
python bigquery_cost_analysis.py
```

## Outputs
- `bigquery_cost_analysis.png` - Visual cost analysis with 4 charts
- `bigquery_cost_estimate.csv` - Detailed monthly breakdown

## Alternative Scenarios Analyzed
1. Keep all data (base scenario)
2. No long-term pricing (all active storage)
3. Delete data after 1 year

## Visualizations Include
1. Total data accumulation over time
2. Monthly cost growth
3. Storage distribution (active vs long-term)
4. Cost breakdown by storage type

---

**Author:** Sherif Elashmawy  
**Date:** January 2026  
**Pricing Source**: [Google Cloud BigQuery Pricing](https://cloud.google.com/bigquery/pricing#storage)