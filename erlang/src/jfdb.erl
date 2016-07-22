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
         keys/1,
         keys/2,
         keys/3,
         fetch/2,
         annul/2,
         annul/3,
         store/3,
         store/4,
         store/5,
         query/2,
         query/3,
         flush/1,
         crush/1,
         close/1]).

-export([call/3]).

%% Object
-export([paths/1,
         paths/2,
         paths/3,
         exists/2,
         lookup/2,
         lookup/3,
         remove/2,
         remove/3,
         assign/3,
         assign/4,
         assign/5,
         modify/3,
         search/2,
         search/3,
         fold/3]).

%% Debug
-export([bin/1,
         key/1,
         pre/1,
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

pre(Path) ->
    case key(Path) of
        <<>> ->
            <<>>;
        Key ->
            binary:part(Key, {0, size(Key) - 1})
    end.

querify({Det, Terms}) when Det =:= any; Det =:= all; Det =:= but ->
    {Det, [querify(T) || T <- Terms]};
querify({pre, Path}) ->
    pre(Path);
querify(Path) ->
    key(Path).

unkey(K) when is_binary(K) ->
    case binary:split(K, <<0>>, [global, trim]) of
        [Key] ->
            Key;
        Path ->
            Path
    end;
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

open(Path, Opts) when is_binary(Path) ->
    open(binary_to_list(Path), Opts);
open(Path, Opts) ->
    jfdb_nif:open(Path, Opts).

call(DB, Method, Args) ->
    case jfdb_nif:call(DB, Method, Args) of
        DB = {jfdb, _} ->
            wait(DB);
        Error ->
            Error
    end.

keys(DB) ->
    keys(DB, <<>>).

keys(DB, Key) ->
    keys(DB, Key, []).

keys(DB, Key, Opts) ->
    call(DB, keys, {Key, Opts}).

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

close(DB) ->
    call(DB, close, {}).

paths(DB) ->
    paths(DB, []).

paths(DB, Path) ->
    paths(DB, Path, []).

paths(DB, {pre, Path}, Opts) ->
    [unkey(K) || K <- keys(DB, pre(Path), Opts)];
paths(DB, Path, Opts) ->
    [unkey(K) || K <- keys(DB, key(Path), Opts)].

exists(DB, Path) ->
    lookup(DB, Path) =/= undefined.

lookup(DB, Path) ->
    lookup(DB, Path, undefined).

lookup(DB, {pre, Path}, Default) ->
    decode(fetch(DB, pre(Path)), Default);
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

modify(DB, Path, Fun) when is_function(Fun) ->
    assign(DB, Path, Fun(lookup(DB, Path)));
modify(DB, Path, Val) ->
    assign(DB, Path, Val).

search(DB, Inquiry) ->
    search(DB, Inquiry, [keys, vals]).

search(DB, Inquiry, Opts) ->
    decode(query(DB, querify(Inquiry), Opts)).

fold(Fun, Acc, DB) ->
    %% NB: if NIFs could call Erlang functions, a deep fold might make sense
    %%     as it is, it makes more sense to break up folds into thin slices
    %%     a native fold would also make this use case more efficient though
    lists:foldl(Fun, Acc, jfdb:lookup(DB, [])).
