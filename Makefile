.PHONY: all shell ct clean deps

all:
	rebar3 compile

shell:
	rebar3 shell --apps waterlily

ct:
	rebar3 ct -v --cover

clean:
	rebar3 clean

deps:
	rebar3 deps
