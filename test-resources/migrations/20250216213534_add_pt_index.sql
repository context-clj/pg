-- add_pt_index
--$up
create table users (
  id serial primary key,
  created_at timestamptz,
  updated_at timestamptz,
  nickname text,
  email text,
  password text
);

--$

create table user_sessions (
  id uuid primary key,
  "user" bigint,
  created_at timestamptz,
  updated_at timestamptz
);

--$down

drop table if exists users;

--$

drop table if exists user_sessions;
