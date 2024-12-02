drop table if exists work_${canonical_id_name}_graph_prev;
create table if not exists work_${canonical_id_name}_graph_prev as
select a.* from "cdp_unification_${unif_name}"."${canonical_id_name}_graph_prev" a
inner join
  (
    select follower_id, follower_ns from "cdp_unification_${unif_name}"."${canonical_id_name}_graph_prev"
    except
    select follower_id, follower_ns from "cdp_unification_${unif_name}"."${canonical_id_name}_graph"
  ) b
  on a.follower_id = b.follower_id and a.follower_ns = b.follower_ns
;

drop table if exists ${canonical_id_name}_graph_prev;
create table if not exists ${canonical_id_name}_graph_prev as
select * from work_${canonical_id_name}_graph_prev
union all
select * from ${canonical_id_name}_graph;
