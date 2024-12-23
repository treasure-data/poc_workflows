with columns as (
  select  ARRAY_JOIN(array_agg(distinct column_name), ',') as column_string from prob_eda_input_columns_temp
  where time in (select max(time) from prob_eda_input_columns_temp)
)

select * from columns