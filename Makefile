REBAR      = rebar
CFLAGS     = -Wall -O3

.PHONY: all clean erlang

all:	jfdb test/jfdb test/trie erlang

jfdb:   src/jfdb.c src/db.o src/trie.o src/repl.h
	$(CC) $(CFLAGS) -o $@ $(filter %.c %.o,$^)

test/%: test/%.c src/db.o src/trie.o src/repl.h
	$(CC) $(CFLAGS) -o $@ $(filter %.c %.o,$^)

%.o:    %.c %.h src/jfdb.h
	$(CC) $(CFLAGS) -o $@ $(filter %.c %.o,$^) -c

clean:
	rm -rf `find . -name \*.o`
	rm -rf `find . -name \*.d`
	rm -rf `find . -name \*.dSYM`
	rm -rf jfdb
	rm -rf erlang/ebin erlang/priv
	rm -rf test/jfdb test/trie

erlang: CMD = compile
erlang:
	(cd erlang && $(REBAR) $(CMD))