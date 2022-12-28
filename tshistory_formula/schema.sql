create table "{ns}".dependant (
  sid int not null references "{ns}".registry(id) on delete cascade,
  needs int not null references "{ns}".registry(id) on delete cascade,
  unique(sid, needs)
);

create index "ix_{ns}_dependant_sid" on "{ns}".dependant (sid);
create index "ix_{ns}_dependant_needs" on "{ns}".dependant (needs);


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
