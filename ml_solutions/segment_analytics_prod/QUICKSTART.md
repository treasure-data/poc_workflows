# Segment Analytics Quick Start Guide

## Solution Overview

**Segment Analytics** is a comprehensive CDP analytics solution that enables marketers and campaign analysts to track KPIs for audiences and customer journeys built in Treasure Data's Audience Studio. The solution automatically aggregates metrics across segments, provides performance dashboards, and tracks changes to segment rules over time.

## Steps To Implement

1. **Configure Workflow YAML** `config/input_params.yml` 
2. **Configure Workflow Secret**: Add your Master API key as `secret_key` in the workflow
3. **Update config.json**: Set dashboard sharing and model parameters
4. **Run Initial Test**: Execute with minimal segments to validate setup
5. **Schedule Workflow**: Add scheduling parameters to main .dig file
6. **Upload Dashboard**: Import the .dash template to Treasure Insights


## Configuring input_params.yml

The `config/input_params.yml` file is the primary configuration for the solution. Below is a detailed guide for each section:

### 1. Global Parameters

```yaml
project_prefix: sa_lite         # Prefix for all output tables (keep short)
sink_database: reporting_qsr_prod  # Database where results will be stored
unique_user_id: td_id           # Primary ID for joining customer data
secondary_id_list:              # Additional IDs for complex joins (comma-separated)
api_endpoint: 'api.treasuredata.com'  # TD API endpoint (change for EU/APAC)
folder_depth: '10'              # How deep to scan folder hierarchies
model_config_table: 'datamodel_build_history'  # Tracking table (leave default)
run_type: 'full'                # 'full' or 'inc' for incremental updates
```

**Key Settings:**
- `sink_database`: Must exist and you must have write permissions
- `unique_user_id`: Should match your master customer ID field
- `api_endpoint`: Use 'api.eu01.treasuredata.com' for EU, 'api.treasuredata.co.jp' for Japan

### 2. Execution Controls

```yaml
create_dashboard: 'yes'         # Build Treasure Insights dashboard
cleanup_temp_tables: 'yes'      # Remove temporary tables after run
fresh_run: 'no'                 # 'yes' deletes all previous data (use carefully!)
```

### 3. Filter Parameters (Critical for Performance)

⚠️ **IMPORTANT**: Always set `apply_ps_filters: 'yes'` and configure filters to avoid processing all segments, which can cause memory issues.

```yaml
filters:
  v5_flag: 1                    # 1 for V5 segments, 0 for V4
  apply_ps_filters: 'yes'       # MUST be 'yes' for production use
  ps_to_include: 'qsr'          # REGEXP to match parent segment names
  folders_to_include: 'rfm|campaign|segmentation'  # REGEXP for folder names
  segments_to_include:          # REGEXP for specific segment names
  journeys_to_include:          # REGEXP for journey names
```

**Filter Examples:**
- `ps_to_include: 'prod|main'` - Includes parent segments with "prod" OR "main"
- `folders_to_include: '^test_'` - Only folders starting with "test_"
- Leave blank to include all (not recommended for initial setup)

### 4. Time Range Configuration

```yaml
time_filter_type: interval      # 'interval' or 'range'
time_range_start_date: 2024-02-01  # Start date for 'range' type
time_range_end_date: 2024-12-31    # End date (use 2222-02-02 for latest)
lookback_period: -365d          # For 'interval' type: -30d, -1M, -2w, etc.
```

**Time Filter Options:**
- **interval**: Dynamic lookback from current date (recommended)
- **range**: Fixed date range for historical analysis

### 5. Metrics Configuration

Define source tables and KPIs to aggregate:

```yaml
aggregate_metrics_tables:
  - src_table: gld_qsr_prod.pageviews    # Source table
    output_table: pageview_events_kpis   # Output table name
    unixtime_col: time                   # Timestamp column (must be UNIX time)
    join_key: td_id                      # Join key to customer table
    apply_time_filter: 'no'              # Apply global time filter?
    table_filter: td_id IS NOT NULL      # Pre-aggregation filter
    query_type:                          # Leave blank for standard, 'custom' for complex
    metrics:
      - metric_name: pageviews           # Metric name in dashboard
        agg: count                       # Aggregation type
        agg_col_name: '1'                # Column to aggregate ('1' for COUNT)
        filter:                          # Optional WHERE clause

      - metric_name: ad_clicks
        agg: count
        agg_col_name: '1'
        filter: "REGEXP_LIKE(lower(td_url), 'utm_')"
```

### 6. Custom Query Type

For complex aggregations that can't be expressed with simple GROUP BY logic, use `query_type: 'custom'`:

```yaml
  - src_table: gld_qsr_prod.order_details
    output_table: order_events_kpis
    unixtime_col: order_datetime_unix
    join_key: td_id
    apply_time_filter: 'no'
    table_filter: td_id IS NOT NULL
    query_type: 'custom'              # Enables custom SQL generation
    metrics:
      - metric_name: total_spend
        agg: sum
        agg_col_name: amount
        filter: amount > 0.0
```

**Important**: When using `query_type: 'custom'`, you MUST create a corresponding SQL file:
- File location: `sql/kpis/{output_table}.sql`
- Example: For `output_table: order_events_kpis`, create `sql/kpis/order_events_kpis.sql`

#### Custom SQL File Structure

Your custom SQL file should follow this template:

```sql
WITH METRIC AS (
  SELECT
    TD_TIME_STRING(unixtime_column, 'd!') AS event_date,
    ${unique_user_id},
    'metric_table_name' AS metric_table,
    -- Your custom aggregations here
    SUM(column1) as metric1,
    COUNT(DISTINCT column2) as metric2,
    -- Complex calculations, CASE statements, etc.
    SUM(CASE WHEN condition THEN value ELSE 0 END) as metric3
  FROM your_source_table
  WHERE TD_TIME_STRING(${td.each.unixtime_col}, 'd!') > '${td.last_results.max_date}'
    AND TD_TIME_STRING(${td.each.unixtime_col}, 'd!') < cast(CURRENT_DATE as varchar)
    ${td.each.final_where_clause}
  GROUP BY 1, 2
)
SELECT
  T1.segment_id,
  T1.segment_name,
  METRIC.*
FROM ${project_prefix}_run_query T1
JOIN METRIC ON T1.${unique_user_id} = METRIC.${unique_user_id}
WHERE T1.segment_id in ${td.last_results.segment_ids}
```

#### When to Use Custom Queries

Use custom queries when you need:
- Multiple metrics from the same table with different filters
- Complex CASE WHEN logic
- Multiple aggregation types in one query
- Conditional aggregations
- Window functions or advanced SQL features


**Note**: The metric names in your YAML config don't need to match the column names in your custom SQL. The YAML metrics list is used for documentation and tracking purposes when using custom queries.




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

## Best Practices

### Initial Setup
1. **Start Small**: Begin with 1-2 folders and 5-10 segments
2. **Test Filters**: Verify your REGEXP patterns match intended segments
3. **Monitor Performance**: Check workflow runtime and data volumes
4. **Validate Metrics**: Compare dashboard results with source data

### Performance Optimization
- Always use filters to limit scope
- Enable `cleanup_temp_tables` to manage storage
- Use `run_type: 'inc'` for daily updates after initial load
- Set appropriate `lookback_period` to limit data scanning

### Adding New Metrics
1. Add new source table configuration under `aggregate_metrics_tables`
2. Define metrics with appropriate aggregations
3. Use `filter:` parameter for conditional metrics
4. Test with a small segment subset first

### Common Filter Patterns
- Include multiple values: `'value1|value2|value3'`
- Exclude pattern: `'^(?!.*exclude_this).*'`
- Case-insensitive: Filters automatically use `lower()`
- Wildcards: Use `.*` for any characters

## Troubleshooting

### Memory Issues
- Reduce number of segments with stricter filters
- Decrease `lookback_period`
- Set `run_type: 'inc'` after initial load

### Missing Data
- Verify `join_key` matches across tables
- Check `unixtime_col` is properly formatted
- Ensure source tables have recent data
- Validate filter patterns aren't too restrictive

### API Errors
- Confirm `api_endpoint` matches your region
- Verify Master API key is set as `secret_key` in workflow
- Check network connectivity to TD API

## Next Steps

1. **Configure Workflow Secret**: Add your Master API key as `secret_key` in the workflow
2. **Update config.json**: Set dashboard sharing and model parameters
3. **Run Initial Test**: Execute with minimal segments to validate setup
4. **Schedule Workflow**: Add scheduling parameters to main .dig file
5. **Upload Dashboard**: Import the .dash template to Treasure Insights

For additional customization or support, contact your Treasure Data representative.