export ERLANG_MK ?= $(CURDIR)/erlang.mk

PROJECT = jfdb
PROJECT_DESCRIPTION = Log-structured trie database w/ {key, value, indices} semantics
PROJECT_VERSION = 0.0.1

CFLAGS += -I../src -std=c99
SOURCES = c_src/db.c c_src/trie.c c_src/jfdb_nif.c
C_SRC_ENV = env.mk
C_SRC_OUTPUT = priv/jfdb_nif

all:: $(ERLANG_MK)
$(ERLANG_MK):
	curl https://erlang.mk/erlang.mk | make -f -

include $(ERLANG_MK)
