-- with_error
--$up
create table tmp (id serial primary key);
--$
ups
--$down
drop table if exists tmp;
