SELECT '${table.db}.${table.name}' as table_name, 
ARRAY${col_pair}[1] as column_name,
ARRAY${col_pair}[2] as col_pair,
corr(ARRAY${col_pair}[1], ARRAY${col_pair}[2]) as pair_corr,
FROM '${table.db}.${table.name}'