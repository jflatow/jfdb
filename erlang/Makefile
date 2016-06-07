export ERLANG_MK ?= $(CURDIR)/erlang.mk

PROJECT = jfdb
PROJECT_DESCRIPTION = Log-structured trie database w/ {key, value, indices} semantics
PROJECT_VERSION = 0.0.1

LDLIBS += ../src/db.o ../src/trie.o
CFLAGS += -I../src
C_SRC_ENV = env.mk
C_SRC_OUTPUT = priv/jfdb_nif

all:: $(ERLANG_MK)
$(ERLANG_MK):
	curl https://erlang.mk/erlang.mk | make -f -

include $(ERLANG_MK)
