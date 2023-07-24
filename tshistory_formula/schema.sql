create table "{ns}".dependent (
  sid int not null references "{ns}".registry(id) on delete cascade,
  needs int not null references "{ns}".registry(id) on delete cascade,
  unique(sid, needs)
);

create index "ix_{ns}_dependent_sid" on "{ns}".dependent (sid);
create index "ix_{ns}_dependent_needs" on "{ns}".dependent (needs);
