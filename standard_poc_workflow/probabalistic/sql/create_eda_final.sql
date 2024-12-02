SELECT T1.*, 
DENSE_RANK() OVER (PARTITION by session_id ORDER BY column_name) as col_idx,
DENSE_RANK() OVER (ORDER BY session_id DESC) as session_idx
FROM prob_eda_input_columns_temp T1