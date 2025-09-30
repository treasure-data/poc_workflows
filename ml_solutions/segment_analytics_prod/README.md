# Solution Overview

The purpose of this solution is to allow CDP users (marketers, campaign analysts, etc.) to track important KPIs for the Audiences or CJO Journeys they build and activate for marketing campaigns in Treasure Data Audience Studio. The Dashboard allows to filter and compare specific segments by Segment Name and Time Period. Some of the OTB metrics include: 

- High Level Summary of Counts and Population sizes for Parent Segment, Audience Studio Folders, Audience Studio Segments, and Journeys and Journey Stages in the given TD account

- Filter Segments by Parent Segment Name, Folder Name, Segment Type and track KPIs below::

- Segment Population growth over time (Daily, Weekly, Monthly etc.)

- Total & AVG Order Value / Spend

- Total & AVG Order Size (or item count)

- Total & AVG number of pageviews

- Total & AVG count of ad-clicks (pageviews with `utm_params` in the URL)

- Total & AVG count of ***social, search, cpc etc.*** ad-clicks (pageviews with `utm_medium/source=social / cpc / search` in the URL)

- Total & AVG number of email events (opens/clicks etc.)

- Track Business Rules Changes for each segment (If TD user Edited the logic for the segment creation in the UI) 

- More Custom Metrics can be added after installation, by following the structure of the `aggregate_metrics_tables:` param in the `config/input_params.yml`


## Use Cases

 - Track Campaign KPIs across Audiences build in Treasure Data
 - Gain valuable business insights and optimize marketing campaigns
 - Easily track daily performance of A/B Testing Segments or Journey Stages in CJO
 - Catch unauthorized Segment Rule changes that might affect population size and performance negatively and revert back to previous logic
 - Measure and communicate the business value of CDP across your org

# Prerequisites and Limitations

* TD Account must have ***Treasure Insights enabled*** 
* TD User isntalling the package must have ***Edit Data Models & Dashboards*** TI permissions in TD
* TD User isntalling the package must have ***Workflow Edit & Run*** permissions in TD and must have access to the databases where the tables with the KPI metrics live.
* It is recommended that you monitor ***datamodel size*** monthly in the V5 console under the `Insights Models` tab and ensure it does not exceed ***5GB*** over time. This should not happen with the OTB configuration, so please contact your TD rep in the rare event that this occurs. 


# Configuring Workflow Parameters

:warning: ***secret_key:*** Once workflow is uploaded to TD account, please ensure you copy-paste your Master Key as a Workflow Secret and save it in the WF project as:  `secret_key` so all the API-based processes can run.

The workflow is set-up, so that the end user only needs to configure the `.yml` file in the `config\` folder and the `config.json` file in main project folder and the Workflow can be ran to execute end-to-end code automatically. You can modify these params after installation manually when further customization of the solution is needed to fit additional data context and use cases that might not be covered by the OTB configuration. Please note that this is ***ONLY recommended for technical users*** with some experience with `DigDag and SQL/Python`. If customer does not have such resources available, please reach out to your TD rep and we can help customize some of the code to your needs when possible. Details on how to configure each yml or config file below:

:warning: **Important Note!!!** Please pay special attention to the `apply_ps_filters: yes` param below and make sure it is always set to `yes` and also the correct notation is used for the `ps_to_include:`,  `folders_to_include:` and `segments_to_include:` params. Entering the proper ***REGEXP*** syntax for these params will ensure that the Workflow will only process data for a selected number of Audience Studio folders and Audiences that you want to track metrics for. If you don't apply these filters, the Workflow will try to process information across all Audience Studio Folders/segments, which could result in very large runtime and lots of Presto compute and could eventually result in errors due to memory limits being reached. ***If you are setting up this solution for the first time***, please start by selecting ***1-2 Audience Studio Folders*** with no more than ***5-10 Audiences***, to get a feel for how much data is being processed and how long the solution takes before you start increasing the number of Audiences you want to start tracking KPIs for.


1. `config/input_params.yml` - Controls important parameters which are often applied globally across most of the data sources and processes in the workflow.

```yaml
#####################################################################
########################## GLOBAL PARAMS ############################
#####################################################################
project_prefix: segment_analytics_lite    #(leave as default) this is added as prefix to the name of all output tables, so user can find them easily in the database
sink_database: reporting_prod             #database where all model output tables will be saved
unique_user_id: td_canonical_id           #the main join key that will be used to join behaviors tables to customers table, unless a foreign join key is required - this can be defined in next step
secondary_id_list:                        #Use format 'id1, id2, id3' to list foreign join keys you can later use for joining to behavior source tables without primary unique ID such as canonical_id
api_endpoint: 'api.treasuredata.com'      #Change to eu01 or .co.jp or ap02 if running this in TD accounts in non-US Regions
folder_depth: '10'                              #(leave as default) determines how deeply into nested folders in Audience Studio the code will scan to find the Segments you want to analyze
model_config_table: 'datamodel_build_history'   #(leave as default) stores list of datamodel names and OIDs existing in the TD account used to find if datamodel_name already exists and should only be updated

create_dashboard: 'yes'              #'yes' - will trigger datamodel and dashboard build sub-workflow (change to 'no' if customer does not have TI enabled)
cleanup_temp_tables: 'yes'            #'yes' - will DELETE all temp_tables not used by final dashboard (leave as Default)

#####################################################################
######################## FILTER PARAMS ############################## <--- NOTE** It is VERY important to apply these correctly when running this for customers with lots of Parent Segments / Audiences
#####################################################################
filters:
  v5_flag: 1                            #'1' - will only scan segments in V5, 0 - only in V4
  apply_ps_filters: 'yes'               #'yes' - applies filters below and only returns info on specific Parent Segments, Folders, or Segments and Journeys, 'no' - returns all objects in Audience Studio
  ps_to_include: 'ml_'               #use lower letter REGEXP notation to only scan selected Parent Segments that include 'ml_' in their name (leave blank to scan all)
  folders_to_include: 'nba|rfm|cjo'   #use lower letter REGEXP notation to only scan selected Audience Studio Folders that include 'rfm OR nba OR cjo' in their name  (leave blank to scan all)
  segments_to_include:                  #use lower letter REGEXP notation such as 'segment_name|segment_name_2|segment_name_3' to only scan selected Segments (leave blank to scan all)
  journeys_to_include: 'nbp'            #use lower letter REGEXP notation such as 'journey_name|journey_name_2|etc.' to only scan selected Journeys (leave blank to scan all)
  time_filter_type: range               #Use 'range' to specify fixed start/end dates OR 'interval' to lookback days/weeks/months from time_range_end_date
  time_range_start_date: 2022-10-26     #Defines start date for`TD_TIME_RANGE` use format YYYY-MM-DD
  time_range_end_date: 2222-02-02       #To always end TIME_RANGE at the latest available date, leave default as '2222-02-02'. To end on a custom date use format YYYY-MM-DD
  lookback_period: -120d                #-1M looksback a month, -30d looks back 30 days, - 2w looksback 2 weeks etc. Used only when time_filter_type = 'interval'

#####################################################################
############# TABLE PARAMS FOR SOURCE AND OUTPUT TABLES #############
#####################################################################

aggregate_metrics_tables:
  - src_table: gldn.enrich_pageviews    #database.table name to aggreagte the KPI from
    output_table: pageview_events_kpis  #name of the output table where the kpis will be stored for this behavior type
    unixtime_col: time                  #UNIXTIME timestamp column for time-filtering and date-grouping
    join_key: td_canonical_id           #unique customer_id
    apply_time_filter: 'yes'            #when set to 'yes' the time_filter_type range from the global filters param above will be applied to these KPIs
    metrics:
      - metric_name: pageviews         #name of metric as you want it to appear in the final table and the Dashboard filters
        agg: count                     
        agg_col_name: '1'              #We use '1' for COUNT instead of '*' because in some instances we might want to SUM(agg_col_name)
        filter:                       # Add custom 'WHERE' CLAUSE filter if needed, see next metrics for an example

      - metric_name: ad_clicks
        agg: count
        agg_col_name: '1'             
        filter: REGEXP_LIKE(lower(td_url), ''utm_'')     #NOTE filter syntax must be added without the WHERE clause and if you filter by a string value, you msut use double single-quotes on each side for Presto to insert filter as a VARCHAR

 - src_table: gldn.enrich_orders       
    output_table: order_events_kpis
    unixtime_col: time                 
    join_key: td_canonical_id           
    apply_time_filter: 'yes'
    metrics:
      - metric_name: total_spend
        agg: sum
        agg_col_name: unit_price
        filter: REGEXP_LIKE(lower(order_status), ''complete'')   #NOTE we're only summing unit_price for orders WHERE order_status contains 'complete' to count true revenue and exclude spend on returned/canceled orders etc.

      - metric_name: orders_count
        agg: approx_distinct           #NOTE we're using 'APPROX_DISTINCT' here to get the distinct order_id counts for the order_count per user_id metric)
        agg_col_name: order_id
        filter: REGEXP_LIKE(lower(order_status), ''complete'')
```

2. `config.json` - Controls important parameters used by Python Custom Scripting `.py` files to create the final JSON for the Datamodel and build & share it via API.

```json
"model_name":  "segment_analytics_lite_auto_prod" (name of datamodel)
,
"model_tables": [
                ] (list of tables to be added to datamodel)
,
"shared_user_list": [ "ps-ml-analytics@treasure-data.com"] (list of users to share datamodel with)
,
"change_schema_cols": {"date": ["event_date","run_time"], "text": ["ENTER_NAME"], "float": ["ENTER_NAME"], "bigint": ["ENTER_NAME"]} (list of columns you want to change datatype from raw table to datamodel. Ex. in "date" you provide column names that will be converted to `datetime`)
,
"join_relations": {"pairs":[]
                  }
} (if any joins were required you can add a list of table_name:join_key pairs)
```


# Explanation of Workflow Tasks, Code and Table Outputs

## DigDag Tasks Summary

- ***segment_analytics_lite_launch.dig*** - runs the main project workflow, that triggers entire project execution end to end, including all sub-workflows and queries in project folder.

- ***segment_analytics_lite_scan_parent_segments.dig***  - scans the Segment API and extracts all the Parent Segment Names, Folder Names, Audience Segment Names and their corresponding IDs, Populations and Query Rules and stores them in a table in TD. This allows us to dynamically scan this table later when we want to aggregate metrics for a selected set of Segments/Audiences.

- ***segment_analytics_lite_data_prep.dig***  - reads from the `input_params.yml` and executes a list of sub-workflows that will dynamically extract the canonical_ids of the desired segment populations from Audience Studio, by running the query_rules stored in the the Segment API JSON for each Segment, and then aggregating the KPIs defined in the `${aggregate_metrics_tables}` list in the `input_params.yml`. These processes are controlled by the sub-workflows:

 - ***segment_analytics_lite_map_segments_to_profiles.dig***  - retrieves all the segment names, ids and the query logic that we have selected using our `${filter}` params in the `input_params.yml` and the  ***segment_analytics_lite_ps_stats*** table. After that it runs each query to get the list of `canonical_ids` for each Segment and loops through all the tables in the `${aggregate_metrics_tables}` list and aggregates the desired metrics by Segment Name, grouping by the extracted canonical_ids.

- ***segment_analytics_lite_datamodel_create.dig***  - creates the TI datamodel via API that powers the Dashboard by reading from the `config.json file` and shares the datamodel with the user emails lsited in the config.

- ***segment_analytics_lite_datamodel_build.dig***  - updates the existing datamodel/dashboard with the latest data generated by each workflow run.


## SQL Queries

- ***sql/filter_segments.sql*** - this reads the params `${filters}` from the `input_params.yml` config file and selects only the `ids` and `names` of the Parent Segments, Folders, and Audiences that the end user wants to make available in the final dashboard for analysis. See snippet below: 

```sql
AND REGEXP_LIKE(lower(ps_name), '${filters.ps_to_include}')
AND REGEXP_LIKE(lower(folder_name), '${filters.folders_to_include}')
AND REGEXP_LIKE(lower(segment_name), '${filters.segments_to_include}')
```
- ***sql/segments_dashboard_get_query.sql*** - this reads from the `segment_analytics_segment_profile_mapping_temp` table and extracts the SQL Query Syntax behind building each audience in the Audience Studio in order to get the `canonical_ids` that belong to each Audience and inserts the ids into the `segment_analytics_run_query` table along with the corresponding Audience Name, ID flag, and population counts.

- ***sql/segments_dashboard_segment_profile_mapping.sql*** - reads from the `${project_prefix}_segment_profile_mapping_temp` and creates the final ***${project_prefix}_segment_profile_mapping*** table that contains important information about each Audience extracted from the Audience Studio API such as population size, the SQL syntax logic for creating each segment, segment_id, segment_name etc. 

- ***sql/parse_table_params.sql*** - parses the JSON params from the `${aggregate_metrics_tables}` param from the `config/input_params.yml` and dynamically generates queries to create the initial KPI tables for each src_table and each metric. 

- ***sql/kpis/create_output_table.sql*** - runs the dynamically generated SQL syntax to insert the latest KPIs based on the max_date of the last run, if the table already exists in the database.

- ***sql/kpis/create_output_table_${filters.time_filter_type}.sql*** - applies the global time filter if the ***apply_time_filter:*** param = 'yes' and runs the dynamically generated SQL syntax to insert the latest KPIs based on the max_date of the last run, if the table already exists in the database.
  
- ***sql/kpis/base_table_syntax.sql*** - parses the JSON params from the `${aggregate_metrics_tables}` param from the `config/input_params.yml` and dynamically generates queries to read from the individual ***output_kpi*** tables, created in previous step and union them into one combined_kpi table with a standerdized schema, which allows you to universally isnert any metric of any type to the same table that will power the final datamodel, allowing you to create global filters and dynamically populate Widgets that won't break even if you keep adding new custom metrics over time. 

- ***sql/kpis/base_table_create.sql*** - rns the dynamically generated syntax from above and `INSERT INTO` only the latest incremental KPIs into the standerdized ***${project_prefix}_kpis_combined*** table.

- ***sql/segments_dashboard_metrics_join.sql, sql/segments_dashboard_metrics_join_journeys.sql*** - these two queries join the Segment information from the ***${project_prefix}_segment_profile_mapping*** table with the KPIs from the ***${project_prefix}_kpis_combined*** table. The one that ends in ***_journeys*** runs if customer has `CJO` and is trying to track KPIs on Audiences that are part of CJO.

- ***sql/final_metrics_table.sql, sql/final_metrics_table_first_run.sql*** - creates the final ***${project_prefix}_final_metrics_table*** by checking the query_syntax VARCHAR column from rpeviosu runs for each Segment and flagging the query_change column = `1` if a change in syntax was detected from previous run.  This is the final table that powers the Segment Analytics Dashboard.
  
## Table Outputs

- ***segment_analytics_ps_stats*** - summary of stats across all Parent Segments, Audience Folders, and Audience Segments that exist in V5 or V4 in TD account. This table is used to filter out which Audience Segments you want to include in the Dashboard as well as to present the High-Level Summary statistics in the `TD Account Summary Tab` in the Dashboard.

| ps_id | ps_name |v5_flag|folder_id|folder_name|segment_id|segment_name|segment population|
|-------|---------|-------|---------|-----------|----------|------------|------------------|
| 24145 | Demos   |   1   |51444    |RFM Models |12451     |Top Tier    |1,245,000         |

- ***segment_analytics_lite_final_metrics_table*** - Final Table with all segment metrics that will be shown in the Dashboard pre-aggregated by ps_name, segment_name, folder_name, event_date, etc..

![Screen Shot 2024-02-23 at 2 05 30 PM](https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/3112d0f7-6f50-47b1-ad57-e88853b75dc0)

### Datamodel & Dashboard Overview

The `Segment Analytics Dashboard` reads from an Insights Model build from the params listed in the `config.json` file. By Default this includes the two  tables listed above- ***segment_analytics_lite_ps_stats and segment_analytics_lite_final_metrics_table***.   More tables and metrics can be added upon customer request to fulfill additional Use Case Requirements.

:warning: ***Segment_Analytics_Template.dash*** --> The Dashboard tempalte file can be found in the Repo Project folder --> `dashboard`. The `.dash` file can be downlaoded locally and then uploaded to any TD account from the Treasure Insights UI (screenshot below):

![Screen Shot 2024-02-23 at 3 22 33 PM](https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/b78d01d8-1eef-49e2-8456-2cbd19b3d143)

- After you upload the `.dash` template a lot of the widgets will show blank, unless you point the new dashboard to the correct datamodel. Use screenshot below as a guideline on how to select the correct datamodel:

![Screen Shot 2024-02-23 at 3 25 16 PM](https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/e894d8c2-3348-419d-8583-785a2d9e5834)


### Dashboard Overview



1. **TD Account Parent Segment & Audience Studio Summary** - 
High Level Summary of Counts and Population sizes for `Parent Segment, Audience Studio Folders, and Audience Studio Segments, Journeys etc.` in the given TD account.


<img width="999" alt="main_screen" src="https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/e9ef228a-c84b-4ce1-a828-f757646818d5">


2b. **Audience Population Metrics** - 
Provides Widgets for KPIs such as `Query Logic Changes Tracker, Latest Population by Segment Name, Daily/Weekly Population Growth Tracker etc.`


<img width="999" alt="Screen Shot 2024-02-23 at 10 40 34 AM" src="https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/d1de4f53-37db-445c-8f0f-8636cc153180">


3. **Web Activity Metrics** - 
Provides Widgets for tracking `Total, Unique & AVG` Daily/Weekly web-events for each Segment Audience. It also shows the ***Latest*** Totals and AVGs of each metric compared by Segment Name. This allows to find Segments that `generate a lot more web-engagement` for the customer, track `how web-activity grows over time` and `measure the effect of different marketing initiatives` on web engagement.

<img width="999" alt="Screen Shot 2024-02-23 at 10 41 20 AM" src="https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/8d0e5718-4103-4460-bba8-34190578bc1e">


4. **Orders/Sales Metrics** - 
Provides Widgets for tracking `Total, Unique & AVG` Daily/Weekly Revenue for each Segment Audience. It also shows the ***Latest*** Totals and AVGs of each metric compared by Segment Name. This allows to find Segments that `generate a lot more sales and revenue` for the customer, track `how Sales/Revenue grows over time` and measure the effect of different `marketing initiatives on revenue growth`.

<img width="999" alt="Screen Shot 2024-02-23 at 10 42 23 AM" src="https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/d2d4a7f8-ff1c-4c32-a2fa-eaa876e3a44e">


5. **Email Activity Metrics** - 
Provides Widgets for tracking `Total, Unique & AVG` Daily/Weekly email-activity (`sends, opens, clicks`) for each Segment Audience. It also shows the ***Latest*** Totals and AVGs of each metric compared by Segment Name. This allows to find Segments that `generate a lot more email engagement` for the customer, track `how email activity grows over time`.

<img width="999" alt="Screen Shot 2024-02-23 at 10 41 48 AM" src="https://github.com/treasure-data-ps/segment_analytics_lite_external/assets/40249921/1a8d8004-bc52-40df-a842-c4f9a36f18b8">


# Additional Information

### 1. How to schedule workflow?
Find the installed workflow project and locate the `segment_analytics_launch.dig` file, which is the main project file that runs the entire end-to-end solution. Click `Launch Project Editor` on the top right in the console and then click `Edit File` on bottom right. At the very top of the file you might see an empty section as such:

In order to schedule the workflow, please add syntax below, so the final syntax looks as such:
```yaml
###################### SCHEDULE PARAMS ##################################  
timezone: UTC

schedule:
  daily>: 07:00:00
```
The above example will automatically run the workflow ***every day at 7:00am*** UTC time. For more scheduling options, please refer to the DigDag documentation at [Link](https://docs.digdag.io/scheduling_workflow.html#setting-up-a-schedule)

### 2. What should I do if I need Tech Support on setting up the solution in my TD account?

Please contact your TD representative and they will be able to dedicate an Engineering resource that can help guide you through the installation process and help you with any troubleshooting or code customizations if needed. 


