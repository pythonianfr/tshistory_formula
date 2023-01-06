create table "{ns}".dependent (
  sid int not null references "{ns}".registry(id) on delete cascade,
  needs int not null references "{ns}".registry(id) on delete cascade,
  unique(sid, needs)
);

create index "ix_{ns}_dependent_sid" on "{ns}".dependent (sid);
create index "ix_{ns}_dependent_needs" on "{ns}".dependent (needs);


create table "{ns}".group_formula (
  id serial primary key,
  -- name will have an index (unique), sufficient for the query needs
  name text unique not null,
  text text not null,
  metadata jsonb
);

create table "{ns}".group_binding (
  id serial primary key,
  -- groupname will have an index (unique), sufficient for the query needs
  groupname text unique not null,
  seriesname text not null,
  binding jsonb not null,
  metadata jsonb
);
