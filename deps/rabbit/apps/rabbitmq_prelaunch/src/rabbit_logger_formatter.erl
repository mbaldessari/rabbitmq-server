%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_logger_formatter).

-export([format/2]).

format(#{msg := Msg, meta := Meta} = LogEvent, Config) ->
    Prefix = format_prefix(LogEvent, Config),
    FormattedMsg = format_msg(Msg, Meta, Config),
    prepend_prefix_to_msg(Prefix, FormattedMsg, Config).

format_prefix(#{level := Level,
                meta := #{time := Timestamp,
                          pid := Pid}},
              Config) ->
    Time = format_time(Timestamp, Config),
    io_lib:format("~ts [~ts] ~p ", [Time, Level, Pid]).

format_time(Timestamp, _) ->
    Options = [{unit, microsecond},
               {time_designator, $\s}],
    calendar:system_time_to_rfc3339(Timestamp, Options).

format_msg({string, Chardata}, Meta, Config) ->
    format_msg({"~ts", [Chardata]}, Meta, Config);
format_msg({report, Report}, Meta, Config) ->
    FormattedReport = format_report(Report, Meta, Config),
    format_msg(FormattedReport, Meta, Config);
format_msg({Format, Args}, _, _) ->
    io_lib:format(Format, Args).

format_report(
  #{label := {application_controller, _}} = Report, Meta, Config) ->
    format_application_progress(Report, Meta, Config);
format_report(
  #{label := {supervisor, progress}} = Report, Meta, Config) ->
    format_supervisor_progress(Report, Meta, Config);
format_report(
  Report, #{report_cb := Cb} = Meta, Config) ->
    try
        case erlang:fun_info(Cb, arity) of
            {arity, 1} -> Cb(Report);
            {arity, 2} -> {"~ts", [Cb(Report, #{})]}
        end
    catch
        _:_:_ ->
            format_report(Report, maps:remove(report_cb, Meta), Config)
    end;
format_report(Report, _, _) ->
    logger:format_report(Report).

format_application_progress(#{label := {_, progress},
                              report := InternalReport}, _, _) ->
    Application = proplists:get_value(application, InternalReport),
    StartedAt = proplists:get_value(started_at, InternalReport),
    {"Application ~w started on ~0p",
     [Application, StartedAt]};
format_application_progress(#{label := {_, exit},
                              report := InternalReport}, _, _) ->
    Application = proplists:get_value(application, InternalReport),
    Exited = proplists:get_value(exited, InternalReport),
    {"Application ~w exited with reason: ~0p",
     [Application, Exited]}.

format_supervisor_progress(#{report := InternalReport}, _, _) ->
    Supervisor = proplists:get_value(supervisor, InternalReport),
    Started = proplists:get_value(started, InternalReport),
    Id = proplists:get_value(id, Started),
    Pid = proplists:get_value(pid, Started),
    Mfa = proplists:get_value(mfargs, Started),
    {"Supervisor ~w: child ~w started (~w): ~0p",
     [Supervisor, Id, Pid, Mfa]}.

prepend_prefix_to_msg(Prefix, FormattedMsg, Config) ->
    Lines = split_lines(FormattedMsg, Config),
    [[Prefix, Line, $\n] || Line <- Lines].

split_lines(FormattedMsg, _) ->
    FlattenMsg = lists:flatten(FormattedMsg),
    string:split(FlattenMsg, [$\n], all).
