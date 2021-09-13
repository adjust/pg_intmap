MODULE_big = pg_intmap
EXTENSION = pg_intmap
PG_CONFIG ?= pg_config
DATA = pg_intmap--0.1.sql
OBJS = pg_intmap.o parser.o
REGRESS = basic
REGRESS_OPTS = --inputdir=test
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
