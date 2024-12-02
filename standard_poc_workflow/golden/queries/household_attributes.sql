with micros_sales AS (
    SELECT 
        household_unification_id,
        SUM(sales_amount) AS sales_amount,
        SUM(sales_quantity) AS sales_quantity
    FROM enriched_micros_menuitemsales
    GROUP BY 1
),
gc_sales AS (
    SELECT
        household_unification_id,
        SUM(sales_amount) AS sales_amount,
        SUM(sales_quantity) AS sales_quantity
    FROM enriched_gc_sales
    GROUP BY 1
),
indi_spend as (
  select household_unification_id,
        SUM(sales_amount) AS sales_amount,
        SUM(sales_quantity) AS sales_quantity
  from (
    select * from micros_sales
    union all
    select * from gc_sales
  ) a
  group by household_unification_id
),
attendence as (
    select distinct household_unification_id,  ticket_id
    FROM enriched_gc_sales
    where ticket_id is not null
    union all 
    select distinct household_unification_id, ticket_id
    FROM enriched_gc_attendance
    where ticket_id is not null
),
tot_att as (select household_unification_id, count(distinct ticket_id) total_attendence
from attendence
group by 1)
select COALESCE(a.household_unification_id , b.household_unification_id) household_unification_id, sales_amount, sales_quantity, total_attendence
from indi_spend a full outer join  tot_att b 
on (a.household_unification_id = b.household_unification_id)