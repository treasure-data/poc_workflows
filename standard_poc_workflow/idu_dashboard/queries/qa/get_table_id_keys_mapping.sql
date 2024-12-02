SELECT
  T3.table_id,
  '${source_db}.enriched_'||T1.table_name AS unification_table,
  T1.column_name as column_name_src,
  T2.key_type,
  T1.key_name as column_key,
  T4.invalid_texts,
  T4.valid_regexp
FROM ${source_db}.column_lookup T1 
  inner join ${source_db}.${canonical_id_col}_keys T2 on T1.key_name = T2.key_name
  inner join ${source_db}.${canonical_id_col}_tables T3 on T1.table_name = T3.table_name
  inner join ${source_db}.filter_lookup T4 on T1.key_name = T4.key_name