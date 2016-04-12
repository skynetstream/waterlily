-record(message, { final = false :: boolean()
                 , to_read = 0   :: non_neg_integer()
                 , buffer = []   :: list(binary())
                 , data = <<>>   :: binary()
                 }).

-ifdef(WL_NO_LOGS).
-define(ERROR(Format, Data), begin Format, Data, ok end).
-define(EMERGENCY(Format, Data), begin Format, Data, ok end).
-define(ALERT(Format, Data), begin Format, Data, ok end).
-define(CRITICAL(Format, Data), begin Format, Data, ok end).
-define(WARNING(Format, Data), begin Format, Data, ok end).
-define(INFO(Format, Data), begin Format, Data, ok end).
-define(NOTICE(Format, Data), begin Format, Data, ok end).
-define(DEBUG(Format, Data), begin Format, Data, ok end).
-else.
-ifdef(WL_LAGER).
-compile({parse_transform, lager_transform}).
-define(EMERGENCY(Format, Data), lager:emergency(Format, Data)).
-define(ALERT(Format, Data), lager:alert(Format, Data)).
-define(CRITICAL(Format, Data), lager:critical(Format, Data)).
-define(ERROR(Format, Data), lager:error(Format, Data)).
-define(WARNING(Format, Data), lager:warning(Format, Data)).
-define(NOTICE(Format, Data), lager:notice(Format, Data)).
-define(INFO(Format, Data), lager:info(Format, Data)).
-define(DEBUG(Format, Data), lager:debug(Format, Data)).
-else.
-define(ERROR(Format, Data), error_logger:error_msg(Format ++ "~n", Data)).
-define(EMERGENCY(Format, Data), ?ERROR(Format, Data)).
-define(ALERT(Format, Data), ?ERROR(Format, Data)).
-define(CRITICAL(Format, Data), ?ERROR(Format, Data)).
-define(WARNING(Format, Data), error_logger:warning_msg(Format ++ "~n", Data)).
-define(INFO(Format, Data), error_logger:info_msg(Format ++ "~n", Data)).
-define(NOTICE(Format, Data), ?INFO(Format, Data)).
-define(DEBUG(Format, Data), ?INFO(Format, Data)).
-endif.
-endif.

-define(EMERGENCY(Format), ?EMERGENCY(Format, [])).
-define(ALERT(Format), ?ALERT(Format, [])).
-define(CRITICAL(Format), ?CRITICAL(Format, [])).
-define(ERROR(Format), ?ERROR(Format, [])).
-define(WARNING(Format), ?WARNING(Format, [])).
-define(NOTICE(Format), ?NOTICE(Format, [])).
-define(INFO(Format), ?INFO(Format, [])).
-define(DEBUG(Format), ?DEBUG(Format, [])).
