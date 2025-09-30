# RFM Model Quick Start Guide

## Configuration Overview

The RFM (Recency, Frequency, Monetary) model is configured through `config/input_params.yml`. This file controls all aspects of the model workflow.

### Key Configuration Sections

#### 1. Global Parameters
```yaml
globals:
  canonical_id: td_id                  # Primary user identifier
  sink_database: gld_ml_stellantis     # Output database in Treasure Data
  model_type: 'automl'                 # 'automl' or 'custom' - use custom if automl is not part of contract
  time_zone: 'UTC'                     # Timezone for processing
```

#### 2. Output Tables - Don't Change these 
```yaml
union_activity_table: rfm_combined_user_events
input_table: rfm_input_table
output_table: rfm_output_table
stats_table: rfm_stats
```

#### 3. Time Filtering (Optional) 
Reduce Volume if Workflow takes long
```yaml
apply_time_filter: 'no'              # Set 'yes' to limit data timeframe
time_filter_type: interval           # 'range' or 'interval'
lookback_period: '-180d'             # Examples: -30d, -2w, -1M
```

#### 4. Data Sources
Configure multiple behavior tables with weighted values:
```yaml
aggregate_metrics_tables:
  - src_table: database.table_name
    name: 'activity_name'
    unixtime_col: timestamp_column - make sure its in unix
    join_key: user_id_column (make sure this is same across all tables)
    order_amount: 100.0              #$ value for this activity
    custom_filter: "optional SQL filter"
```

## ⚠️ IMPORTANT: Email Configuration Required

**Before running the workflow**, you must configure user emails in `config.json`:

```json
{
  "shared_user_list": ["user1@company.com", "user2@company.com"]
}
```

**Critical**: All users in the `shared_user_list` array MUST have Data Model access in Treasure Insights. Without proper access, you will encounter this error:

```
File "/home/td-user/.local/lib/python3.9/site-packages/td_ml_datamodel_create/create_datamodel.py", line 166, in create_model
    model_dic = dict(name = [resp['name']], oid = resp['oid'], created_by = resp['created_by'],
KeyError: 'name'
```

## Workflow Overview

1. **Data Collection**: Combines multiple customer interaction sources (orders, leads, web visits, etc.)
2. **Feature Engineering**: Calculates recency, frequency, and monetary values per customer
3. **Segmentation**: Assigns customers to RFM segments based on their behavior
4. **Scoring**: Generates propensity scores using AutoML or custom models
5. **Output**: Creates scored customer tables in the specified database

## Getting Started

1. Update `config/input_params.yml` with your:
   - Database names
   - Table mappings
   - Activity weights
   - Time filters (if needed)

2. Update `config.json` `shared_user_list` with valid TI emails

3. Run the workflow in Treasure Data

4. Find results in your specified `sink_database`:
   - `rfm_output_table`: Final scored customer data
   - `rfm_stats`: Model performance statistics
   - `rfm_combined_user_events`: Unified activity data