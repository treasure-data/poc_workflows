
-- -- ---Exploding and Ranking: The lists of ids associated with each block_key are then "exploded" into separate rows. After this, a rank is assigned to each block_key and a row number (duplicate_count) for each id within a block.
-- -- --  below id list is exploded to rows from list
-- -- -- e.g
-- -- -- block_key1  id1
-- -- -- block_key1  id60 .... end so on

WITH T5 AS (
    SELECT
        block_key,
        exploded.id
    FROM
        ${blocking_table}_temp4
    LATERAL VIEW explode(userid_set) exploded AS id
),
T6 AS (
    SELECT
        T5.*,
        rank() OVER (ORDER BY T5.block_key) AS rnk
    FROM T5
)

-- -- ---Join and Filter: Finally, it joins the result with the input_table using the id_col. It filters out rows where duplicate_count is 2 or more, meaning it's eliminating duplicate rows within each block. The final output is the original data from input_table with an additional block_key and rnk (rank) field added for each id_col.

-- DIGDAG_INSERT_LINE

SELECT 
    T6.block_key, 
    org.*, 
    T6.rnk 
FROM 
    T6 
LEFT JOIN 
    ${source_db}.${input_table} org
ON 
    T6.id = org.${id_col}


