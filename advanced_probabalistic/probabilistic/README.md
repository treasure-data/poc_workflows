
# General Overview:
Probabilistic matching is a solution for identifying `text/numeric` entries that are approximately similar, but not exactly the same, for use in customer profile ID matching when deterministic 1-to-1 matches are not sufficient. Our Probabilistic Matching Algorithms is designed to be applied after our Deterministic ID Unification is complete to further reduce number of duplicate profiles, improve customer analytics and reduce $$$ waste on ad spend. Our workflow uses hashing/similarity functions in Presto/Hive and DigDag parallelism to ensure maximum compute efficiency and scalability on Big Data Volume before the final Python algorithms are ran inside parallel docker containers. Below are some of the advantages that our custom solution offers compared to just using a standard fuzzy matching approach:

- Allows end user to assign weights to the different fields used for matching in order to define which IDs should have more priority in the final matching logic

- Uses a combination of hashing and string similarity algorithms, whose combined outputs are more accurate and balanced than using a standard approach

- Allows end user to control the threshold for how strict the similarity algorithms should be, which gives the flexibility to achieve the right balance between deduplication % and the strength of the similarity  between the final deduplicated IDs

- Allows end user to define a custom logic and apply it as a final filter to the deduplicated population, to control which matches would be “accepted” as true and used in production.

- Visualize model outputs in a TI Dashboard to track deduplication metrics over time and tweak deduplication parameters accordingly

- Track $$$ savings from the deduplication process, by entering an avg_marketing_spend_per_user custom variable in the config file.

# Tech Requirements:

Requires understanding of ***Presto/Hive, DigDag, Python Custom Scripting and Treasure Insights***.

# Data Requirements:

- Please make sure that you have at least one production ready table from list below in TD:

- Have already ran our OTB ID Unification algorithm first.  Probabilistic is usually ran after deterministic algorithm is complete.

- Have source tables with at least 1 or more columns with text/numeric entries that are approximately similar, but not exactly the same (ex. address, phone, email, firstname_lastname etc.), where 1-to-1 exact matches would fail in OTB ID Unification.

# Tech Implementation Summary:

The workflow is set-up, so that the end user only needs to configure the `.yml` files in the `config\` folder and the `config.json` file in main project folder and the wf can be ran to execute end-to-end code automatically. Details on how to configure each yml or config file below:

### config/global.yml:

Controls important parameters which are often applied globally across most of the data sources and processes in the workflow.

```YML
sink_database: ml_dev
model_config_table: datamodel_build_history

input_table: sample_dedupe_1m (input table to be deduplicated)
id_col: td_canonical_id (main_id)
output_table: dedupe_final_clusters (final table with deduped IDs)
cluster_col_name : cluster_id (name of column with deduped clusters)

record_limit: 500,000 (how many records will be processed in a single docker image)

###### Columns used for Unification and their Types #######

dedupe_columns: (below you define colums you want to used for matching and their weights)
- name: phone
  type: phone
  weight: 0.6
  final_approval: 'yes'  #when set to 'yes' , then the final acceptance logic will be applied to this column in the 'sql/acceptance_create_final_table.sql' query

- name: email
  type: email
  weight: 0.2
  final_approval: 'no'

- name: td_screen
  type: string
  weight: 0.2
  final_approval: 'no'
   

### Increasing the value of hashes,keygroups and jaccard_similarity_threshold will increase the Recall
### hashes----> Number of hash functions used in Hive code
### keygroups----> Number of hash values used for creating a signature in Hive
### We recommend a ratio of 3:1 or less between hashes/keygroups
#### jaccard_similarity_threshold----> minimum jaccard similarity threshold 
to create blocks. The higher it is, the more strict the algorithm will be for 
clsuter_id. Default recommended: 0.5

hashes: 8
keygroups: 3
jaccard_similarity_threshold: 0.5

#### Params for clustering and similarity , use string_type cosine or jarowinkler

#convergence_threshold ---> just controls Soft Impute for NULL values and it is not as important to modify, so we can run as default. 
It would only have big impact when clsuters have many records and lots of records have NULL values. 

#cluster_threshold ---> bw 0-1, so the closer to 1.0 it is, the more strict the 
clustering logic would be, thus reducing number of clusters taht qualify for matching, 
but increasing similarity between records in each cluster. Recommended values: 0.45-0.80

#string_type ---> use string_type cosine or jarowinkler, Default: jarowinkler since it is a bit more 
compute efficient. Use cosine if you are matching a bigger corpus of text.

##fill_missing ---> Default: True --> applies Soft Impute to NULL values to improve clustering scoring. 
When set to == False, it will give you 0 similarity when values = NULL and reduce clustering score.

#avg_spend_per_user --> allows to define how much customer spends on AVG per year to send marketing 
campaigns to a single ID, which determines the estimated savings, shown in the final dashboard.

convergence_threshold : 0.01
cluster_threshold: 0.65
string_type : jarowinkler 
fill_missing: True
avg_spend_per_user: 150
```

### config.json:

Controls important parameters used by Python Custom Scripting .py files to create the final JSON for the Datamodel, which is then sent to datamodel build endpoint via POST API Request.

```
"model_name":  "prob_unification_automated" (name of datamodel)
,
"model_tables": [
                ] (list of tables to be added to datamodel)
,
"shared_user_list": [ "dilyan.kovachev+psdemo@treasure-data.com"] (list of users to share datamodel with)
,
"change_schema_cols": {"date": ["run_date", "event_time"], "text": ["ENTER_NAME"], "float": ["ENTER_NAME"], "bigint": ["ENTER_NAME"]} (list of columns you want to change datatype from raw table to datamodel. Ex. in "date" you provide column names that will be converted to `datetime`)
,
"join_relations": {"pairs":[]
                  }
} (if any joins were required you can add a list of table_name:join_key pairs)
```

# Solution Architecture

<img width="777" alt="sa_ui_1" src="https://user-images.githubusercontent.com/40249921/185972254-fcc12ea7-09f9-42fd-a57f-33cc5d80d7c0.png">


### DigDag Files Summary:

- ***ml_prob_unification_blocking.dig*** - runs all Presto/Hive queries that read the raw input table and execute data cleaning, n-grams, min-hashing, and blocking functions to prepare it for the final dedupe fastclsutering algorithm in Python.  

- ***ml_prob_unification_python.dig*** - picks up the prob_dedupe_cluster_table generated by previous workflow and runs the Python algorithms to create the final cluster table of deduped IDs.  Note: this workflow runs only if the data volume of the prob_dedupe_cluster_table is below the ${record_limit} var set in the global.yml file to ensure scalability of the Python code.

- ***ml_prob_unification_parallel_python.dig*** - Does the same as the previous .dig but only in case when the prob_dedupe_cluster_table is very large and exceeds the ${record_limit} var. In this instance, the prob_dedupe_cluster_table input table is broken down into smaller chunks = ${record_limit} and multiple docker containers are executed in parallel to allow Python code to process large volume of records without reaching memory limits.

- ***ml_prob_datamodel_create.dig*** - creates the datamodel that powers the Dashboard by reading from the config.json file.

- ***ml_prob_datamodel_update.dig*** - updates the existing datamodel/dashboard with the latest data generated by each workflow run.

### Table Outputs:

- ***prob_dedupe_blocking_table*** - table that brakes original matching fields into 3-5 character n-grams and creates min-hashes of each by applying the Presto/Hive functions to the original raw table that needed to be deduplicated

- ***prob_dedupe_cluster_table*** - table that calculates the jaccard_similarity between the different mini-hashes and uses a similarity score threshold to cluster similar records into blocks.

- ***prob_dedupe_final_clusters*** - the final table of deduplicated IDs after the Python dedupe functions and the custom matching field weights are applied to the records in each block in the dedupe_cluster_table to cluster the final records that pass cluster_similarity_threshold.

- ***prob_model_table_params*** - table that tracks historic deduplication metrics and custom parameters used for each workflow run, used to create Dashboard visualizations of model outputs.

- ***prob_final_cluster_histogram*** - table that tracks the distribution of how many records fall in the final blocks/clusters. Seeing too many records per cluster could signify that matching might not be strict/accurate enough and custom logic and similarity thresholds must be increased.

- ***prob_session_global_filter*** - used for creating global dashboard date & session_idfilters by joining to the individual tables in the datamodel on session_id.

# Probabilistic ID Unification Dashboard Overview:

The Prob ID Unification  Dasbhoard reads from an `Insights Model` build from the params listed in the `config.jso`n file. By Default this includes two of the tables listed above - ***prob_model_table_params, prob_final_cluster_histogram, and prob_session_global_filter***, but more tables can be added if further KPIs are required. 

**NOTE:** If you'd like to use the Pre-Built Dashboard Template from the screenshots below, please download the Workflow project locally and locate the file `dashboards/Prob_ID_Unification.dash`. The `.dash` file needs to be imported **manually** into Treasure Insights by going to the Dashboards Toolbar in the CDP UI and clicking on `+ --> Import Dashboards` as in screenshot below:

<img width="333" alt="sa_ui_1" src="https://user-images.githubusercontent.com/40249921/185973504-c75976e0-bf30-4f11-9ea0-a1099db93383.png">

**IMPORTANT**: In order for the Template to work OTB, you need to open the Dashboard you uploaded and click on  Change Data Source and select the datamodel that was built by the workflow as below:

<img width="333" alt="sa_ui_1" src="https://user-images.githubusercontent.com/40249921/185973530-9db92904-3427-4d05-8bf0-5e6c9debd934.png">


## 1. ID Unification Summary

Provides Widgets for KPIs such as **Last Date Run, Workflow Duration, Total Profiles in Raw Table, Profiles Eligible for Dedupe, Deduped Profiles, Dedupe Rate, Annual Savings and Custom Model Parameters*** defined by end user in the global.yml params file.

<img width="777" alt="sa_ui_1" src="https://user-images.githubusercontent.com/40249921/185973770-f079eba5-5b4f-40ae-8ac8-fab7de67cb3d.png">

## 2. Deduplication Rate & Savings Tracker

Provides Widgets for tracking ***Daily/Weekly/Monthly Deduplication Metrics and Savings*** generated by the deduplicated IDs, which will no longer be targeted multiple times.  See screenshot below:

<img width="777" alt="sa_ui_1" src="https://user-images.githubusercontent.com/40249921/185974220-8cc2ab55-dd7b-4e11-a995-fa09a0430635.png">

## 3. Unification Cluster Stats

Provides Widgets for monitoring the population distribution of the final Blocks and Clusters that are generated by the Presto/Hive and Python code.  These widgets help determine whether matching logic is strict/accurate enough and we’re not deduping too many records that might not have enough similarity to be counted as the same person. See screenshot below:

<img width="777" alt="sa_ui_1" src="https://user-images.githubusercontent.com/40249921/185974331-675e2eb0-8725-4c90-b416-1d8f2ca0e7c1.png">

## 4. Global Filters
The Dashboard can be filtered OTB by MTA Model, Date, and Country but any other important filter can be added

<img width="111" alt="sa_ui_1" src="https://user-images.githubusercontent.com/40249921/185974398-02e001d2-b368-46e1-9bd4-9436c5292751.png">












