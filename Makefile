# contrib/pg_part/Makefile

EXTENSION = pg_fkpart
DATA = pg_fkpart--0.1.1.sql pg_fkpart--unpackaged--0.1.1.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_fkpart
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
