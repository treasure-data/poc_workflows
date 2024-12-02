
drop table if exists ${dest_db}.${unif_src_tbl};

create table if not exists ${dest_db}.${unif_src_tbl} as
select * from ${dest_db}.${dest_tbl};
