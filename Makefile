# contrib/pg_part/Makefile

EXTENSION = pg_fkpart
DATA = pg_fkpart--1.2.sql pg_fkpart--unpackaged--1.0.sql pg_fkpart--1.0--1.1.sql pg_fkpart--1.1--1.2.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
