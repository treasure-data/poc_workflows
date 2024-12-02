INSERT INTO prob_dedupe_blocking_schema
SELECT '${column_name.name}' as column_name,

'SPLIT_PART(trim(lower(CAST(${column_name.name} as varchar))) , ''@'',1) as ${column_name.name}' as cleaning_query,

-- 'CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, 1,3) ELSE NULL END as ${column_name.name}' as predicate_query
'crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, 1,2) ELSE NULL END AS varbinary))) as ${column_name.name}_first_two ,
crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, -2) ELSE NULL END AS varbinary))) as ${column_name.name}_last_two,
crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, -3) ELSE NULL END AS varbinary))) as ${column_name.name}_last_three,
crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, -5,3) ELSE NULL END AS varbinary))) as ${column_name.name}_last_five ,
crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, 1,3) ELSE NULL END AS varbinary))) as ${column_name.name}_first_three ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, 2,3) ELSE NULL END AS varbinary))) as ${column_name.name}_first_bigram ,
crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name},3,4) ELSE NULL END AS varbinary))) as ${column_name.name}_second_two_bigram,
crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name},4,5) ELSE NULL END AS varbinary))) as ${column_name.name}_third_two_bigram,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN SUBSTRING(${column_name.name}, 1,5) ELSE NULL END AS varbinary))) as ${column_name.name}_first_five' as predicate_query,

'${column_name.name}' as predicate_keys,
CAST(${column_name.weight} AS VARCHAR) AS col_weight



