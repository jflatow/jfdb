-module(jfdb).

%% Rough types
-type key()     :: binary().
-type path()    :: list() | atom() | binary() | string().
-type term(T)   :: {any | all | but, [term(T)]} | T.
-type query()   :: term(key()).
-type inquiry() :: term(path()).

-export_type([key/0,
              path/0,
              term/1,
              query/0,
              inquiry/0]).

%% Raw
-export([open/1,
         open/2,
         fetch/2,
         annul/2,
         annul/3,
         store/3,
         store/4,
         store/5,
         query/2,
         query/3,
         flush/1,
         crush/1]).

-export([call/3]).

%% Object
-export([lookup/2,
         lookup/3,
         remove/2,
         remove/3,
         assign/3,
         assign/4,
         assign/5,
         search/2,
         search/3]).

%% Debug
-export([bin/1,
         key/1,
         querify/1,
         unkey/1,
         unval/1,
         decode/1,
         decode/2]).

bin(Atom) when is_atom(Atom) ->
    list_to_binary(atom_to_list(Atom));
bin(List) when is_list(List) ->
    list_to_binary(List);
bin(Bin) when is_binary(Bin) ->
    Bin.

key(Key) when not is_list(Key) ->
    <<(bin(Key))/binary, 0>>;
key(Str = [C|_]) when is_integer(C) ->
    <<(bin(Str))/binary, 0>>;
key(Path) when is_list(Path) ->
    << <<(bin(X))/binary, 0>> || X <- Path >>.

querify({Det, Terms}) when Det =:= any; Det =:= all; Det =:= but ->
    {Det, [querify(T) || T <- Terms]};
querify(Path) ->
    key(Path).


unkey(K) when is_binary(K) ->
    binary:split(K, <<0>>, [global, trim]);
unkey(K) ->
    K.

unval(V) when is_binary(V) ->
    binary_to_term(V);
unval(V) ->
    V.

decode(Enc) ->
    decode(Enc, undefined).

decode(undefined, Default) ->
    Default;
decode({K, V}, _) ->
    {unkey(K), unval(V)};
decode(<<V/binary>>, _) ->
    unval(V);
decode(KVs, Default) when is_list(KVs) ->
    [decode(KV, Default) || KV <- KVs].

wait(DB) ->
    receive
        {jfdb, ok} ->
            DB;
        {jfdb, Reply} ->
            Reply
    end.

open(Path) ->
    open(Path, []).

open(Path, Args) ->
    jfdb_nif:open(Path, Args).

call(DB, Method, Args) ->
    case jfdb_nif:call(DB, Method, Args) of
        DB when is_binary(DB) ->
            wait(DB);
        Error ->
            Error
    end.

fetch(DB, Key) ->
    call(DB, fetch, Key).

annul(DB, Key) ->
    annul(DB, Key, []).

annul(DB, Key, Opts) ->
    call(DB, annul, {Key, Opts}).

store(DB, Key, Val) ->
    store(DB, Key, Val, []).

store(DB, Key, Val, Indices) ->
    store(DB, Key, Val, Indices, []).

store(DB, Key, Val, Indices, Opts) ->
    call(DB, store, {Key, Val, Indices, Opts}).

query(DB, Query) ->
    query(DB, Query, [keys, vals]).

query(DB, Query, Opts) ->
    call(DB, query, {Query, Opts}).

flush(DB) ->
    call(DB, flush, {}).

crush(DB) ->
    call(DB, crush, {}).

lookup(DB, Path) ->
    lookup(DB, Path, undefined).

lookup(DB, Path, Default) ->
    decode(fetch(DB, key(Path)), Default).

remove(DB, Path) ->
    remove(DB, Path, []).

remove(DB, Path, Opts) ->
    annul(DB, key(Path), Opts).

assign(DB, Path, Val) ->
    assign(DB, Path, Val, []).

assign(DB, Path, Val, Indices) ->
    assign(DB, Path, Val, Indices, []).

assign(DB, Path, Val, Indices, Opts) ->
    store(DB, key(Path), term_to_binary(Val), [key(I) || I <- Indices], Opts).

search(DB, Inquiry) ->
    search(DB, Inquiry, [keys, vals]).

search(DB, Inquiry, Opts) ->
    decode(query(DB, querify(Inquiry), Opts)).
