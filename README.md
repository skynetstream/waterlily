waterlily
=====

An OTP library

Build
-----

    $ make

How to use
----------

    $ {ok, Pid} = waterlily:connect(fun(R) -> io:format("Response: ~p") end).
    $ waterlily:query(Pid, <<"select * from schemas;">>).
