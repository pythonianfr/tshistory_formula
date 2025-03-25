create table "{ns}".dependent (
  sid int not null references "{ns}".registry(id) on delete cascade,
  needs int not null references "{ns}".registry(id) on delete cascade,
  unique(sid, needs)
);

create index "ix_{ns}_dependent_sid" on "{ns}".dependent (sid);
create index "ix_{ns}_dependent_needs" on "{ns}".dependent (needs);


-- formula history
create table "{ns}".form_history (
  sid int not null references "{ns}".registry(id) on delete cascade,
  archivedate timestamptz not null default now(),
  formula text not null
);

create index "ix_{ns}_form_history_sid" on "{ns}".form_history(sid);


-- bound groups

create table "{ns}".group_binding (
  id serial primary key,
  groupid int not null references "{ns}".group_registry(id) on delete cascade,
  seriesid int not null references "{ns}".registry(id) on delete cascade
);

create index "ix_{ns}_group_binding_groupid" on "{ns}".group_binding (groupid);
create index "ix_{ns}_group_binding_seriesid" on "{ns}".group_binding (seriesid);


create table "{ns}".group_series_map (
  family text not null,
  parent int not null references "{ns}".group_binding(id) on delete cascade,
  groupid int not null references "{ns}".group_registry(id) on delete cascade,
  seriesid int not null references "{ns}".registry(id) on delete cascade,
  unique(family, groupid, seriesid)
);

create index "ix_{ns}_group_series_map_parent" on "{ns}".group_series_map (parent);
create index "ix_{ns}_group_series_map_family" on "{ns}".group_series_map (family);
create index "ix_{ns}_group_series_map_groupid" on "{ns}".group_series_map (groupid);
create index "ix_{ns}_group_series_map_seriesid" on "{ns}".group_series_map (seriesid);
