
with T1 as (
  SELECT Clusterid, ${id_col} from  ${blocking_table}_temp 
  where cluster_rank BETWEEN  ${td.each.range_start} and ${td.each.range_end}
)

-- DIGDAG_INSERT_LINE
SELECT 
  L.${id_col},
  R.${id_col} AS ${id_col}s,
  COUNT(*) AS cnt 
FROM
  T1 l LEFT OUTER
JOIN
   T1  R
  ON   L.Clusterid = R.Clusterid
WHERE
  L.${id_col} < R.${id_col}
GROUP BY
  l.${id_col},
  r.${id_col}
