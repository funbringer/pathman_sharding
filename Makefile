# contrib/pathman_sharding/Makefile

MODULE_big = pathman_sharding

OBJS = src/pathman_sharding.o src/hooks.o $(WIN32RES)

PG_CPPFLAGS = -I$(CURDIR)/src/include

EXTENSION = pathman_sharding

EXTVERSION = 0.1

DATA_built = pathman_sharding--$(EXTVERSION).sql

PGFILEDESC = "pathman_sharding - Improved sharding for pg_pathman"

REGRESS = pathman_sharding

SHLIB_LINK += -lpq

EXTRA_CLEAN =

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pathman_sharding
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--$(EXTVERSION).sql: init.sql
	cat $^ > $@
