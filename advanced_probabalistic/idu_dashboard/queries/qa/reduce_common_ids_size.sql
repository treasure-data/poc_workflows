with top_ids as (
  select over_merged_id, count(distinct id) as id_count from idu_qa_common_ids   
  group by 1 
  limit 50
)

select * from idu_qa_common_ids where over_merged_id in (select over_merged_id from top_ids)

-- WITH RankedIDs AS (
--   SELECT
-- T1.*,
--     ROW_NUMBER() OVER (PARTITION BY over_merged_id, id_type ORDER BY total_sets DESC) AS rn
--   FROM idu_qa_common_ids T1
-- )
-- SELECT
-- *
-- FROM RankedIDs
-- WHERE rn <= 10