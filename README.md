waterlily
=====

An OTP library

Build
-----

    $ make

How to use
----------

    $ {ok, Pid} = waterlily:connect().
    $ Respone = waterlily:query(Pid, <<"select * from schemas;">>).
