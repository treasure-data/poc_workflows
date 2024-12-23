INSERT INTO prob_dedupe_blocking_schema
SELECT  '${column_name.name}' as column_name,

'trim(lower(CAST(${column_name.name} as varchar))) as ${column_name.name}' as cleaning_query,

'crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 1,3),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_first_two ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 2,6),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_second_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 3,6),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_third_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 3,8),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_third_eight_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 5,8),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_fifth_eight_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 9,11),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_nine_elavan_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 10,13),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_tenth_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 14,17),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_forteen_seventeen_gram ,


crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 18,22),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_eighteen_twotwo_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 22,24),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_twenty_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 6,9),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_fourth_gram ,
crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, 10,14),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_fifth_gram ,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, -5,2),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_last_five_two,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, -10,3),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_last_ten_three,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, -15,4),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_last_fifteen_four,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, -2),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_last_ten,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, -3),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_last_five,

crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, -4),'''') ELSE NULL END AS varbinary))) as ${column_name.name}_last_seven'   

AS predicate_query,

'${column_name.name}' as predicate_keys,

CAST(${column_name.weight} AS VARCHAR) AS col_weight


--,crc32(xxhash64(CAST(CASE WHEN ${column_name.name} IS NOT NULL THEN NULLIF(SUBSTRING(${column_name.name}, -15) ELSE NULL END AS varbinary))) as ${column_name.name}_last_five