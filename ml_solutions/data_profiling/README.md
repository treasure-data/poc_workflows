> Written with [StackEdit](https://stackedit.io/)
# Package Overview

This solution can serve ***most teams working with customer data*** in the CDP such as `Marketers, Data Scientists, Data Engineers, Business Decision Makers, and Privacy / Compliance Officers`. 

For `Marketers and Business Decision Makers` it can be used as an ***Audience Insights*** tool, when applied to the `customers` table of a ***Parent Segment***, as it will help visualize the statistics and data distribution of important customer `attributes & behaviors`, which can help `understand the characteristics of your entire customer population` and `inform Audience building strategies`. 

It can serve `Data Scientists` and `Data Engineers` as a very scalable ***Data Profiling / Exploratory Data Analysis*** tool, which can speed up the `Data Q&A, Data Prep, Outlier Handling, Feature Engineering, Feature Seletion, and ML Model Training and Optimization` tasks.  

For `Privacy Compliance officers` it can help ***detect and remove PII*** from any table in the CDP. 

The workflow is built using only ***Presto/Hive*** functions and ***DigDag*** to ensure `maximum compute efficiency` and `scalability on Big Data Volume`. The outputs of the wf enable some of the helpful insights below:

- Get a general overview of the dataset statistics for any table in CDP

- Serve as an ***Audience Insights*** tool, when applied to the `customers` table of a ***Parent Segment***, as it will help visualize the statistics and data distribution of important customer `attributes & behaviors`

- Have an easy way to decide which tables/columns might be worth bringing to Master Segment and which seem to have mostly ***noise, messy data, or NULL values***.

- Provide Data Science and Data Engineering teams with a quick view of which tables/columns could be useful when setting up ML-Models and building robust Dashboards

- For numerical data it will show statistical summary including -  ***KDE plot, BoxPlot, MAX, Q1, Q2, AVG, Q3, MAX, Kurtosis, Skewness, Outliers, and linear correlation.***

- For string data it will show ***distinct value_counts(), num of columns with PII, numerical, datetime*** or other non-standard data.

- For timestamp data it will show ***start_date, end_date, time_interval range, and will plot top-k days with the most activity*** from each table.

## Use Cases

 - Perform ***quick and easy data validation***, after ingesting source tables to CDP to ensure there was not ***data loss/corruption*** during data trasnfer
 - Detect columns that cointain ***PII*** to ensure ***privacy compliance***
 - Perform easy ***EDA*** for `Data Scientists / Data Analysts` to understand the distribution and quality of the data
 - Understand ***correlation*** between columns, which can help marketers build more intelligent Audiences and Data Scientists build ML Models


# Prerequisites and Limitations

* TD Account must have ***Treasure Insights enabled*** 
* TD User isntalling the package must have ***Edit Data Models & Dashboards*** TI permissions in TD
* TD User isntalling the package must have ***Workflow Edit & Run*** permissions in TD and must have access to the databases where the tables with the KPI metrics live.
* It is recommended that you monitor ***datamodel size*** monthly in the V5 console under the `Insights Models` tab and ensure it does not exceed ***4GB*** over time. This should not happen with the OTB configuration, so please contact your TD rep in the rare event that this occurs. 

### Data Requirements:
Please make sure that you have at least one production ready table from list below in TD:

- Any table with a unique user_id and ***contextual or quantitative*** data that you are trying to `understand the distribution` of and `scan for PII, missing values, stats, data distribution, correlations` etc.

- Preferably have at least one ***Parent Segment*** built in TD, as this tool can help you `very quickly understand the high-level statistics of all users` and their attributes/behaviors in the PS.


# Explanation of Config Params and How to Modify After Installation if Needed

The workflow is set-up, so that the end user only needs to configure the `config/global.yml` and the `config.json` files in the main project folder. Details on what each parameter does below:


1. `config/global.yml` - Controls important parameters which are often applied globally across most of the data sources and processes in the workflow.

- **cleanup_temp_tables:** yes - when set to yes temporary tables will be deleted at the end of workflow run.

- **get_db_table_list:** yes - when set to yes it will scan INFORMATION_SCHEMA and will return list of all table names in the selected database.

- **table_list:** - following syntax below add list of table_name, database, columns to exclude from calculations, and name of the true timestamp column. These will be the tables that the wf will loop through to perform EDA and output final column stats for.

```
############# TABLE LIST ################
table_list:             #below list all tables you want to perform data-profiling for

  - name : td_attributes_mock    #name of table to scan
    db : ml_dev                 #name of database where table lives
    exclude_cols: time|canonical_id     #column names to exclude from data-profiling
    date_col: time                 #unixtime col name

  - name : rfm_mock_data
    db : ml_dev
    exclude_cols: time|canonical_id
    date_col: time
```

- **include_dbs_tbs:** WHERE REGEXP_LIKE(table_schema, '${sink_database}') - regexp to extract list of table names from database names that match REGEXP syntax.

- **metadata_table:**  eda_tables_column_metadata - name of the final metadata table that contains column schema and datatypes for each table from the table_list

- **head_sample:**  5 - count of how many data samples you want to include in the column_sample field of the data summary table.

- **top_k_vals:**  10 - determines how many of the top-k distinct value counts will be aggregated for each VARCHAR column

- **min_rows:**  5 - determines the min number of NON NULL rows that must be present in order for column to be considered

- **exclude_pii:**  ##AND pii_flag = 0 - if value is uncommented, it will add an AND clause to query that does aggregations for VARCHAR to exclude column with PII data.

- **data_threshold:**  0.25 - used as a min percentage of total data required to match REGEXP, when running PII Detection and other data detection code.

- **numeric_flag_threshold:** 0.85 - what % of the data must contain only digits and signs BUT no letters to be flagged as numeric column

- **sample_size:**  20 - determines what percent of the total data to do a TABLESAMPLE BERNOULLI for before running the data detection code.

- **num_bins:**  50 - determines num bins when aggregating numerical data for Histogram/KDE plots

- **top_k_days_by_event_cnt:**  30 - determines top-k days to aggregate by event_count

- **date_range_format:**  'day' - determines if datetime column will be aggregated by day, month, year etc.

2. `config.json` - Controls important parameters used by Python Custom Scripting `.py` files to create the final JSON for the Datamodel and build & share it via API.

```"model_name":  "ml_eda_pckg_test" (name of datamodel)
,
"model_tables": [
    {"db":"ml_dev","name":"eda_tables_column_metadata"},
    {"db":"ml_dev","name":"eda_date_column_stats"},
    {"db":"ml_dev","name":"eda_numerical_column_stats"},
    {"db":"ml_dev","name":"eda_varchar_column_stats"},
    {"db":"ml_dev","name":"eda_numerical_column_histogram"},
    {"db":"ml_dev","name":"eda_num_corr_matrix"}
                ] (list of tables to be added to datamodel)
,
"shared_user_list": [ "firstname.lastname@treasure-data.com"] (list of users to share datamodel with)
,
"change_schema_cols": {"date": ["ENTER_NAME"], "text": ["ENTER_NAME"], "float": ["ENTER_NAME"], "bigint": ["ENTER_NAME"]} (list of columns you want to change datatype from raw table to datamodel. Ex. in "date" you provide column names that will be converted to `datetime`)
,
"join_relations": {"pairs":[
      {"db1": "ml_dev", "tb1":"eda_tables_column_metadata","join_key1":"table_name","db2": "ml_dev","tb2":"eda_date_column_stats","join_key2":"table_name"},
    {"db1": "ml_dev","tb1":"eda_tables_column_metadata","join_key1":"table_name","db2": "ml_dev","tb2":"eda_numerical_column_stats","join_key2":"table_name"}
]
                  }
} (if any joins were required you can add a list of table_name:join_key pairs)
```

# What Tasks Are Ran by the Workflow and What are the Outputs the End User can Access?

### DigDag Tasks Summary

- ***ml_eda_launch.dig*** - runs the main project workflow, that triggers entire project execution end to end, including all sub-workflows and queries in project folder.

- ***ml_eda.dig***  - runs all the ***data-scanning processes and queries*** that perform the data-profiling, PII-detecting, and mathematical functions that save the data summaries to the output tables that power the Dashboard.

- ***ml_eda_create_datamodel.dig***  - creates the datamodel that powers the Dashboard by reading from the `config.json file`.

- ***ml_eda_refresh_model_build.dig***  - updates the existing datamodel/dashboard with the latest data generated by each workflow run.


### Table Outputs

- **eda_tables_column_metadata** - the final metadata table that contains all the datatype flags and PII Detection flags, which are used as global filter in the EDA Dashboard.

| table_name      | column_name |data_type  |sample_values |pii_flag|categorical|tstamp_flag|is_date|is_num|is_unixtime|is_email|
|-----------------|-------------|-----------|--------------|--------|-----------|-----------|-------|------|-----------|--------|
| ml_de.pageviews | country     |  varchar  | 'US, Italy'  |    0   |    0      |    0      |    0  |    0 |    0      |    0   | 

- **eda_varchar_column_stats** - table with list of all VARCHAR column names, datatypes and stats.

| table_name      | column_name |total_rows  |null_cnt |null_perc|distinct_vals|col_value|value_counts|
|-----------------|-------------|-----------|----------|---------|-------------|---------|------------|
| ml_de.pageviews | country     |  12,000   | 1200     |    0.10 |    16       |  'USA'  |    7,456   |  

- **eda_numerical_column_stats** - table with list of all NUMERIC (double or bigint) column names, datatypes and stats.

| table_name      | column_name |total_rows |null_cnt |null_perc|max_value|min_value|avg_value|std_dev| var  |  q1  |  q2  |  q3  | num_outliers|
|-----------------|-------------|-----------|---------|---------|---------|---------|---------|-------|------|------|------|------|-------------|
| ml_de.pageviews | unit_price  |  12,000   | 1200    |    0.10 | 258.45  |  0.00   |  7,456  |12.45  |146.22|106.21|126.26|156.32|   567       |

- **eda_date_column_stats** - table with list of all TIMESTAMP (datetime, date, unixtime, ISO) column names, datatypes and stats.

| table_name      | column_name |null_cnt  |distinct_vals|oldest_date|latest_date|time_range_days|avg_daily_events|event_date|num_events|
|-----------------|-------------|----------|-------------|-----------|-----------|---------------|----------------|----------|----------|
| ml_de.pageviews | time        |  0       |    8762     | 2019-01-02| 2022-01-02|   832         |    8.31        |2020-01-02| 14       |

- **eda_numerical_column_histogram** - table with binned data stats used for KDE and BoxPlot widgets in the EDA Dashboard

| table_name      | column_name |bin label  |bin_cnt   |
|-----------------|-------------|-----------|----------|
| ml_de.pageviews | age         |  37.0     | 258      |

- **eda_num_corr_matrix** - table with CORRELATION values for each column pair in the NUMERIC column list of each table.

| table_name      | column_name | corr_col  |corr_pair  |pair_corr |
|-----------------|-------------|-----------|-----------|----------|
| ml_de.pageviews | age         | cltv      | age_cltv  | 0.264    |

### Datamodel & Dashboard Overview

The `Data Profiling Dashboard` reads from an Insights Model build from the params listed in the `config.json` file. All tables in the datamodel are joined to the ***eda_tables_column_metadata*** on `table_name` and `column_name`, so the main metadata table columns can be used as ***dashboard filters***. The Dashboard is broken down into the four sections below:


### Dashboard Screenshots

1. **General Data Overview** - 
Provides ***high-level overview*** of the data in each TD table listed in the `table list` section of the `config/global.yml`. See screenshot below:

<img width="618" alt="eda_summary" src="https://user-images.githubusercontent.com/40249921/182254695-bc9e5517-4043-41b7-b502-b4a9fbf69179.png">

2. **VARCHAR Columns** - 
Provides ***Data Profiling Stats*** for each `String` column such as `null count, distinct value counts, list of PII columns` etc. See screenshot below:

<img width="618" alt="eda_varchar" src="https://user-images.githubusercontent.com/40249921/182254975-de05b988-68d8-4749-a059-97cd8512d082.png">

3. **NUMERICAL Columns** - 
Provides ***Data Stats (EDA)*** for each `Numeric` column such as `null count, KDE plot, BoxPlot, MAX, Q1, Q2, AVG, Q3, MAX, Kurtosis, Skewness, Outliers, linear correlation` etc. See screenshot below:

<img width="618" alt="eda_numeric" src="https://user-images.githubusercontent.com/40249921/182255036-c5e1a951-0535-41e2-93a6-5e4828b5afad.png">
<img width="618" alt="eda_corr" src="https://user-images.githubusercontent.com/40249921/182255405-5228afbb-29b0-42b6-b4ef-3e058c6952c9.png">

4. **TIMESTAMP Columns** - 
Provides ***Datetime Stats*** for each `TIMESTAMP` column such as `null count, time_format, min_date, max_date, time_range` etc. See screenshot below:

<img width="618" alt="eda_timestamp" src="https://user-images.githubusercontent.com/40249921/182255117-2b76a7ef-1922-459b-aa8f-ab8e13b0b412.png">

5. **Global Filters** - 
The Dashboard can be filtered by `table_name, column_name, datatype, pii_flag, numerical_flag, categorical_flag, timestamp_flag` etc.

<img width="90" alt="eda_filters" src="https://user-images.githubusercontent.com/40249921/182255172-f64cbe27-ebea-470a-9702-24fd13af4472.png">

### Additional Code Examples

#### 1. Important SQL Queries

- ***queries/get_columns_datatype.sql*** - scans each table in the `${table_list}` from the `config/global.yml` and extracts the metadata for all columns in the each table.

- ***queries/get_column_head_sample.sql*** - gets the value samples from each column in each table for the data preview widget of the Dashboard.

- ***queries/get_varchar_data.sql*** - Gets the name of the `VARCHAR` columns in each table and stores them for use of future queries.

- ***queries/column_type_detection_varchar.sql*** - detects PII by using `REGEXP` to detect data such as `email, phone, social security, etc.`

- ***queries/get_original_num_columns.sql*** - Gets the name of the `NUMERIC` columns in each table and stores them for use of future queries.

- ***queries/create_final_metadata_table.sql*** - stores a table with all table and column names and flags the `datatype` as well as column contains `PII` 

- ***queries/get_stats_varchar_data.sql*** - extracts the important statistics of columns with `VARCHAR` data.

- ***queries/get_binned_numerical_data.sql*** - creates a binned historigram of numeric columns to display ***data distribution (KDE Plot)*** in Dashboard.

- ***queries/get_stats_numerical_data.sql*** - extracts the important statistics of columns with `NUMERIC` data. 

- ***queries/get_stats_date_data.sql*** - extracts the important statistics of columns with `TIMESTAMP` data. 

- ***queries/execute_corr_query.sql*** - loops through all combinations of `numeric` columns and calculates the `correlation coefficients` between each pair.

# Additional Information

### 1. How to schedule workflow if it was not scheduled during package install?
Find the installed workflow project using the guide from the paragraph above and identify the `ml_eda_launch.dig` file, which is the main project fil that runs the entire end-to-end solution. Click `Launch Project Editor` on the top right in the console and then click `Edit File` on bottom right. At the very top of the file you might see an empty section as such:
```
###################### SCHEDULE PARAMS ##################################  
```
In order to schedule the workflow, please add syntax below, so the final syntax looks as such:
```
###################### SCHEDULE PARAMS ##################################  
timezone: UTC

schedule:
  daily>: 07:00:00
```
The above example will automatically run the workflow ***every day at 7:00am*** UTC time. For more scheduling options, please refer to the DigDag documentation at [Link](https://docs.digdag.io/scheduling_workflow.html#setting-up-a-schedule)

### 2. Will I lose my project output tables and dashboards  if I delete / uninstall project Workflow?
No. Output Tables, Datamodels or Dashboards created by the workflow will **NOT** be deleted if the WF is deleted. 

### 3. What should I do if I need Tech Support on setting up the package?

Please contact your TD representative and they will be able to dedicate an Engineering resource that can help guide you through the installation process and help you with any troubleshooting or code customizations if needed. 


