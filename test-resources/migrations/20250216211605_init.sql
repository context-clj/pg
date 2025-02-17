--$up
create table patient (
  id serial primary key,
  given text,
  family text,
  birthDate timestamptz,
  gender text
);

--$down
drop table if exists patient;
