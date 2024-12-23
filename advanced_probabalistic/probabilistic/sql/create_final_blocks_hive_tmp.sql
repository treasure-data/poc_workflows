with T2 AS (
    SELECT 
      ${id_col},
       ${id_col}s,
      SUM(cnt) AS sum_cnt -- minhash
  from 
    ${blocking_table}_temp2
    GROUP BY
      1,2
    HAVING
      SUM(cnt) > CAST(round(${hashes}*${jaccard_similarity_threshold}) AS INTEGER) -- Jaccard similarity
),
T3 AS (
    SELECT 
        ${id_col},
        CAST(collect_set(household_unification_ids) AS ARRAY<STRING>) AS userid_set
    FROM T2
    GROUP BY ${id_col}
)

-- DIGDAG_INSERT_LINE
SELECT * FROM T3