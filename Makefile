# contrib/pg_part/Makefile

EXTENSION = pg_fkpart
DATA = pg_fkpart--1.7.sql pg_fkpart--unpackaged--1.1.sql pg_fkpart--1.0--1.1.sql pg_fkpart--1.1--1.2.sql pg_fkpart--1.2--1.3.sql pg_fkpart--1.3--1.4.sql pg_fkpart--1.4--1.5.sql pg_fkpart--1.5--1.6.sql pg_fkpart--1.6--1.7.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
