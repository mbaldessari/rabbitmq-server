%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_logging).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbitmq_prelaunch/include/logging.hrl").

-export([setup/1]).

%% Logging configuration in the `rabbit` Erlang application.
%%
%% {rabbit, [
%%   {log, [
%%     {categories, [
%%       {Category, [
%%         {level, Level},
%%         FileOutputProps
%%       ]},
%%
%%       {default, [
%%         {level, Level}
%%       ]}
%%     ]},
%%
%%     OutputsProps
%%   ]
%% ]}.
-define(DEFAULT_LOG_LEVEL, info).

setup(Context) ->
    ?LOG_DEBUG(""),
    ?LOG_DEBUG("== Logging =="),
    ok = set_ERL_CRASH_DUMP_envvar(Context),
    ok = configure_logger(Context),
    throw(youpi),
    ok.

get_log_base_dir(#{log_base_dir := LogBaseDirFromEnv} = Context) ->
    case rabbit_env:has_var_been_overridden(Context, log_base_dir) of
        false -> application:get_env(lager, log_root, LogBaseDirFromEnv);
        true  -> LogBaseDirFromEnv
    end.

%% -------------------------------------------------------------------
%% ERL_CRASH_DUMP setting.
%% -------------------------------------------------------------------

set_ERL_CRASH_DUMP_envvar(Context) ->
    case os:getenv("ERL_CRASH_DUMP") of
        false ->
            LogBaseDir = get_log_base_dir(Context),
            ErlCrashDump = filename:join(LogBaseDir, "erl_crash.dump"),
            ?LOG_DEBUG(
              "Setting $ERL_CRASH_DUMP environment variable to \"~ts\"",
              [ErlCrashDump],
              #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
            os:putenv("ERL_CRASH_DUMP", ErlCrashDump),
            ok;
        ErlCrashDump ->
            ?LOG_DEBUG(
              "$ERL_CRASH_DUMP environment variable already set to \"~ts\"",
              [ErlCrashDump],
              #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
            ok
    end.

%% -------------------------------------------------------------------
%% Logger's handlers configuration.
%% -------------------------------------------------------------------

configure_logger(Context) ->
    %% Configure main handlers.
    %% We distinguish them by their type and possibly other
    %% parameters (file name, syslog settings, etc.).
    LogConfig0 = get_log_configuration_from_app_env(Context),
    LogConfig1 = handle_default_and_overridden_outputs(LogConfig0, Context),
    LogConfig2 = apply_log_levels_from_env(LogConfig1, Context),
    LogConfig3 = make_filenames_absolute(LogConfig2),
    ?LOG_DEBUG("LogConfig = ~p", [LogConfig3],
               #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    Handlers = create_logger_handlers_conf(LogConfig3),
    ?LOG_DEBUG("Handlers = ~p", [Handlers],
               #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    io:format(standard_error, "Handlers = ~p~n", [Handlers]).

get_log_configuration_from_app_env(Context) ->
    %% The log configuration in the Cuttlefish configuration file or the
    %% application environment is not structured logically. This functions is
    %% responsible for extracting the configuration and organize it. If one day
    %% we decide to fix the configuration structure, we just have to modify
    %% this function and normalize_*().
    Env = application:get_env(rabbit, log, []),
    DefaultAndCatProps = proplists:get_value(categories, Env, []),
    DefaultProps = proplists:get_value(default, DefaultAndCatProps, []),
    CatProps = proplists:delete(default, DefaultAndCatProps),
    PerCatConfig = maps:from_list(
                     [{Cat, normalize_per_cat_log_config(Props)}
                      || {Cat, Props} <- CatProps]),
    EnvWithoutCats = proplists:delete(categories, Env),
    GlobalConfig = normalize_main_log_config(EnvWithoutCats, DefaultProps),
    LogBaseDir = get_log_base_dir(Context),
    #{global => GlobalConfig,
      per_category => PerCatConfig,
      log_base_dir => LogBaseDir}.

normalize_main_log_config(Props, DefaultProps) ->
    Outputs = case proplists:get_value(level, DefaultProps) of
                  undefined -> #{outputs => []};
                  Level     -> #{outputs => [],
                                 level => Level}
              end,
    normalize_main_log_config1(Props, Outputs).

normalize_main_log_config1([{Type, Props} | Rest],
                           #{outputs := Outputs} = LogConfig) ->
    Outputs1 = normalize_main_output(Type, Props, Outputs),
    LogConfig1 = LogConfig#{outputs => Outputs1},
    normalize_main_log_config1(Rest, LogConfig1);
normalize_main_log_config1([], LogConfig) ->
    LogConfig.

normalize_main_output(file, Props, Outputs) ->
    normalize_main_file_output(Props, #{module => logger_std_h,
                                       type => file}, Outputs);
normalize_main_output(console, Props, Outputs) ->
    normalize_main_console_output(Props, #{module => logger_std_h,
                                           type => standard_io}, Outputs).
%% TODO: Normalize other output types.

normalize_main_file_output([{file, false} | _], _, Outputs) ->
    maps:filter(
      fun
          (_, #{module := logger_std_h, type := file}) -> false;
          (_, _)                                       -> true
      end, Outputs);
normalize_main_file_output([{file, Filename} | Rest],
                           Output, Outputs) ->
    Output1 = Output#{file => Filename},
    normalize_main_file_output(Rest, Output1, Outputs);
normalize_main_file_output([{level, Level} | Rest],
                           Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_file_output(Rest, Output1, Outputs);
normalize_main_file_output([], Output, Outputs) ->
    [Output | Outputs].
%% TODO: Normalize other file properties.

normalize_main_console_output([{enabled, false} | _], _, Outputs) ->
    maps:filter(
      fun
          (_, #{module := logger_std_h, type := standard_io})    -> false;
          (_, #{module := logger_std_h, type := standard_error}) -> false;
          (_, _)                                                 -> true
      end, Outputs);
normalize_main_console_output([{enabled, true} | Rest], Output, Outputs) ->
    normalize_main_console_output(Rest, Output, Outputs);
normalize_main_console_output([{level, Level} | Rest],
                              Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_console_output(Rest, Output1, Outputs);
normalize_main_console_output([], Output, Outputs) ->
    [Output | Outputs].

normalize_per_cat_log_config(Props) ->
    normalize_per_cat_log_config(Props, #{}).

normalize_per_cat_log_config([{level, Level} | Rest], LogConfig) ->
    LogConfig1 = LogConfig#{level => Level},
    normalize_per_cat_log_config(Rest, LogConfig1);
normalize_per_cat_log_config([{file, Filename} | Rest], LogConfig) ->
    Output = #{module => logger_std_h,
               type => file,
               file => Filename},
    LogConfig1 = LogConfig#{outputs => [Output]},
    normalize_per_cat_log_config(Rest, LogConfig1);
normalize_per_cat_log_config([], LogConfig) ->
    LogConfig.

handle_default_and_overridden_outputs(LogConfig, Context) ->
    LogConfig1 = handle_default_main_output(LogConfig, Context),
    LogConfig2 = handle_default_upgrade_cat_output(LogConfig1, Context),
    LogConfig2.

handle_default_main_output(
  #{global := #{outputs := Outputs} = GlobalConfig} = LogConfig,
  #{main_log_file := MainLogFile} = Context) ->
    NoOutputsConfigured = Outputs =:= [],
    Overridden = rabbit_env:has_var_been_overridden(Context, main_log_file),
    Outputs1 = case MainLogFile of
                   "-" when NoOutputsConfigured orelse Overridden ->
                       [#{module => logger_std_h,
                          type => standard_io}];
                   Filename when NoOutputsConfigured orelse Overridden ->
                       [#{module => logger_std_h,
                          type => file,
                          file => Filename}];
                   _ ->
                       Outputs
               end,
    case Outputs1 of
        Outputs -> LogConfig;
        _       -> LogConfig#{
                     global => GlobalConfig#{
                                 outputs => Outputs1}}
    end.

handle_default_upgrade_cat_output(
  #{per_category := PerCatConfig} = LogConfig,
  #{upgrade_log_file := UpgLogFile} = Context) ->
    UpgCatConfig = case PerCatConfig of
                       #{upgrade := CatConfig} -> CatConfig;
                       _                       -> #{outputs => []}
                   end,
    #{outputs := Outputs} = UpgCatConfig,
    NoOutputsConfigured = Outputs =:= [],
    Overridden = rabbit_env:has_var_been_overridden(Context, upgrade_log_file),
    Outputs1 = case UpgLogFile of
                   "-" when NoOutputsConfigured orelse Overridden ->
                       %% We re-use the default logger handler.
                       [];
                   Filename when NoOutputsConfigured orelse Overridden ->
                       [#{module => logger_std_h,
                          type => file,
                          file => Filename}];
                   _ ->
                       Outputs
               end,
    case Outputs1 of
        Outputs -> LogConfig;
        _       -> LogConfig#{
                     per_category => PerCatConfig#{
                                       upgrade => UpgCatConfig#{
                                                    outputs => Outputs1}}}
    end.

apply_log_levels_from_env(LogConfig, #{log_levels := LogLevels}) ->
    maps:fold(
      fun
          (color, _, LC) ->
              LC;
          (global, Level, #{global := GlobalConfig} = LC) ->
              GlobalConfig1 = GlobalConfig#{level => Level},
              LC#{global => GlobalConfig1};
          (CatString, Level, #{per_category := PerCatConfig} = LC) ->
              CatAtom = list_to_atom(CatString),
              CatConfig0 = maps:get(CatAtom, PerCatConfig, #{outputs => []}),
              CatConfig1 = CatConfig0#{level => Level},
              PerCatConfig1 = PerCatConfig#{CatAtom => CatConfig1},
              LC#{per_category => PerCatConfig1}
      end, LogConfig, LogLevels).

make_filenames_absolute(
  #{global := GlobalConfig,
    per_category := PerCatConfig,
    log_base_dir := LogBaseDir} = LogConfig) ->
    GlobalConfig1 = make_filenames_absolute(GlobalConfig, LogBaseDir),
    PerCatConfig1 = maps:map(
                      fun(_, CatConfig) ->
                              make_filenames_absolute(CatConfig, LogBaseDir)
                      end, PerCatConfig),
    LogConfig#{global => GlobalConfig1, per_category => PerCatConfig1}.

make_filenames_absolute(#{outputs := Outputs} = Config, LogBaseDir) ->
    Outputs1 = lists:map(
                 fun
                     (#{module := logger_std_h,
                        type := file,
                        file := Filename} = Output) ->
                         Output#{file => filename:absname(Filename, LogBaseDir)};
                     (Output) ->
                         Output
                 end, Outputs),
    Config#{outputs => Outputs1}.

create_logger_handlers_conf(
  #{global := GlobalConfig, per_category := PerCatConfig}) ->
    Handlers0 = create_global_handlers_conf(GlobalConfig),
    Handlers1 = create_per_cat_handlers_conf(PerCatConfig, Handlers0),
    Handlers1.

create_global_handlers_conf(#{outputs := Outputs} = GlobalConfig) ->
    create_handlers_conf(Outputs, global, GlobalConfig, #{}).

create_per_cat_handlers_conf(PerCatConfig, Handlers) ->
    maps:fold(
      fun
          (CatName, #{outputs := []} = CatConfig, Hdls) ->
              filter_cat_in_global_handlers(Hdls, CatName, CatConfig);
          (CatName, #{outputs := Outputs} = CatConfig, Hdls) ->
              Hdls1 = create_handlers_conf(Outputs, CatName, CatConfig, Hdls),
              filter_out_cat_in_other_handlers(Hdls1, CatName)
      end, Handlers, PerCatConfig).

create_handlers_conf([Output | Rest], CatName, Config, Handlers) ->
    Key = create_handler_key(Output),
    Handler = case maps:is_key(Key, Handlers) of
                  false -> create_handler_conf(Output, CatName, Config);
                  true  -> update_handler_conf(maps:get(Key, Handlers),
                                               CatName, Output)
              end,
    Handlers1 = Handlers#{Key => Handler},
    create_handlers_conf(Rest, CatName, Config, Handlers1);
create_handlers_conf([], _, _, Handlers) ->
    Handlers.

create_handler_key(
  #{module := logger_std_h, type := file, file := Filename}) ->
    {file, Filename};
create_handler_key(
  #{module := logger_std_h, type := standard_io}) ->
    {console, standard_io}.

create_handler_conf(Output, global, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{level => Level,
            filter_default => log,
            filters => [],
            formatter => rabbit_prelaunch_early_logging:default_formatter()};
create_handler_conf(Output, CatName, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{filter_default => stop,
            filters => [{CatName, {fun filter_event/2, {CatName, Level}}}],
            formatter => rabbit_prelaunch_early_logging:default_formatter()}.

update_handler_conf(
  #{level := ConfiguredLevel} = Handler, global, Output) ->
    case Output of
        #{level := NewLevel} ->
            case logger:compare_levels(NewLevel, ConfiguredLevel) of
                lt -> Handler#{level => NewLevel};
                _  -> Handler
            end;
        _ ->
            Handler
    end;
update_handler_conf(
  #{filters := Filters} = Handler, CatName, Output) ->
    Level = compute_level_from_config_and_output(Handler, Output),
    Filters1 = [{CatName, {fun filter_event/2, {CatName, Level}}} | Filters],
    Handler#{filters => Filters1}.

compute_level_from_config_and_output(Config, Output) ->
    GeneralLevel = maps:get(level, Config, ?DEFAULT_LOG_LEVEL),
    OutputLevel = maps:get(level, Output, GeneralLevel),
    case logger:compare_levels(OutputLevel, GeneralLevel) of
        lt -> OutputLevel;
        _  -> GeneralLevel
    end.

filter_cat_in_global_handlers(Handlers, CatName, CatConfig) ->
    maps:map(
      fun(_, #{filters := Filters} = Handler) ->
              Level = compute_level_from_config_and_output(
                        Handler, CatConfig),
              Filters1 = [{CatName,
                           {fun filter_event/2, {CatName, Level}}}
                          | Filters],
              Handler#{filters => Filters1}
      end, Handlers).

filter_out_cat_in_other_handlers(Handlers, CatName) ->
    maps:map(
      fun(_, #{filters := Filters} = Handler) ->
              case lists:keymember(CatName, 1, Filters) of
                  true ->
                      Handler;
                  false ->
                      Filters1 = [{CatName,
                                   {fun filter_event/2, {CatName, none}}}
                                  | Filters],
                      Handler#{filters => Filters1}
              end
      end, Handlers).

filter_event(
  #{meta := #{domain := [?LOGGER_SUPER_DOMAIN_NAME, CatName | _]}},
  {CatName, none}) ->
    stop;
filter_event(
  #{level := Level,
    meta := #{domain := [?LOGGER_SUPER_DOMAIN_NAME, CatName | _]}} = LogEvent,
  {CatName, CatLevel}) ->
    case logger:compare_levels(CatLevel, Level) of
        lt -> LogEvent;
        _  -> stop
    end;
filter_event(_, _) ->
    ignore.
