# Retail Demand Forecasting Models

## Staging Models

Transform raw data into standardized format for analytics.

- **stg_census_retail_sales.sql**
  - Cleans Census Bureau retail sales data
  - Validates sales values and state codes
  - Handles missing values and outliers

- **stg_fred_economic.sql**
  - Standardizes economic indicators
  - Handles missing values with previous period data
  - Validates indicator ranges and formats

- **stg_weather_stats.sql**
  - Transforms weather metrics into analysis-ready format
  - Creates temperature and precipitation categories
  - Validates weather measurements

## Intermediate Models

Creates derived metrics and features.

- **int_retail_metrics.sql**
  - Calculates growth rates and moving averages
  - Generates seasonal indices
  - Computes market share metrics

- **int_economic_trends.sql**
  - Creates economic trend indicators
  - Calculates rolling averages of indicators
  - Identifies economic pattern changes

- **int_inventory_metrics.sql**
  - Computes inventory turnover rates
  - Calculates safety stock levels
  - Generates stockout risk metrics

## Marts Models

Business-ready datasets.

- **mart_retail_analysis.sql**
  - Combines sales, economic, and weather data
  - Creates final analysis-ready dataset
  - Includes all dimensions for reporting

- **mart_inventory_planning.sql**
  - Optimizes inventory recommendations
  - Integrates multiple data sources
  - Generates actionable insights

## ML Models

Machine learning feature engineering.

- **feature_engineering.sql**
  - Creates lagged variables
  - Generates interaction features
  - Prepares data for ML models

## Metrics Models

KPI and performance tracking.

- **technical_metrics.sql**
  - Tracks forecast accuracy (MAPE)
  - Monitors system performance
  - Measures data freshness

- **business_metrics.sql**
  - Measures inventory optimization
  - Tracks cost reduction
  - Calculates efficiency improvements

## Snapshots

Track historical changes.

- **retail_sales_snapshot.sql**
  - Tracks sales data changes
  - Maintains history of updates
  - Enables trend analysis

- **economic_data_snapshot.sql**
  - Preserves economic indicator history
  - Tracks indicator revisions
  - Enables backtesting

- **weather_stats_snapshot.sql**
  - Maintains weather data history
  - Tracks pattern changes
  - Supports seasonal analysis

- **retail_metrics_snapshot.sql**
  - Preserves calculated metrics
  - Tracks KPI changes
  - Enables performance analysis

## Data Tests

53 tests ensure data quality:

- Column not null
- Unique combinations
- Referential integrity
- Value ranges
- Custom business rules

## Sources

5 main data sources:

- Census Bureau retail data
- FRED economic indicators
- Weather statistics
- Business performance metrics
- Forecast performance logs

This modular structure enables:

- Clear data lineage
- Efficient processing
- Reliable forecasting
- Easy maintenance
- Quality assurance
- Performance monitoring
