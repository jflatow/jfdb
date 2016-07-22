-module(jfdb_test).

-include_lib("eunit/include/eunit.hrl").

l(I) when is_integer(I) ->
    integer_to_list(I).

basic_test() ->
    basic_test(1000).

basic_test(N) ->
    check_basic(N, store_basic(N, jfdb:open("basic_test", [temporary]))).

store_basic(N, DB = {jfdb, _}) ->
    DB = lists:foldl(
           fun (X, D) when D =:= DB ->
                   jfdb:assign(DB, [l(X rem 10), l(X)], #{map => X})
           end, DB, lists:seq(1, N)),
    DB = lists:foldl(
           fun (X, D) when D =:= DB ->
                   jfdb:assign(DB, [l(X rem 10), l(X rem 10), l(X)], #{map => X})
           end, DB, lists:seq(1, N)),
    DB = jfdb:assign(DB, key, val, [i1]),
    DB = jfdb:assign(DB, <<"this is a key">>, #{fancy => <<"this is a val">>}, [a, x, y, z]),
    DB = jfdb:assign(DB, [this, is, a, path], not_that_fancy, [x, y, z]),
    DB = jfdb:assign(DB, [this, is, another, path], <<"this is not a path">>, [x, y, z]),
    DB = jfdb:assign(DB, [lots, o, keys], {ok, '?'}, [l(I) || I <- lists:seq(1, 1024)]),
    DB = jfdb:assign(DB, k1, v1, [[time, a], [place, p1]]),
    DB = jfdb:assign(DB, k2, v2, [[time, b], [place, p1]]),
    DB = jfdb:assign(DB, k3, v3, [[time, a], [place, p2]]),
    DB = jfdb:assign(DB, k4, v4, [[time, c]]),
    DB = jfdb:assign(DB, k5, v5, [[place, p2]]),
    DB = jfdb:flush(DB).

check_basic(N, DB) ->
    lists:foldl(
      fun (X, _) ->
              ?assertEqual(jfdb:lookup(DB, [l(X rem 10), l(X rem 10), l(X)]), #{map => X})
      end, [], lists:seq(1, N)),
    DB.
