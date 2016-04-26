-module(waterlily_throttle).

-export([ add1/1
        , pow2/1
        , exp/1
        , id/1
        ]).

add1(Throttle) ->
    Throttle + 1.

pow2(0) -> 1;
pow2(Throttle) ->
    Throttle * 2.

exp(0) -> 2;
exp(1) -> 2;
exp(Throttle) ->
    Throttle * Throttle.

id(Throttle) -> Throttle.
