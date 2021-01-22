%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_logging).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbitmq_prelaunch/include/logging.hrl").

-export([setup/1,
         log_locations/0]).

-export_type([log_location/0]).

-type log_location() :: file:name() | string().

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
    ?LOG_DEBUG("~n== Logging ==",
               #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ok = set_ERL_CRASH_DUMP_envvar(Context),
    ok = configure_logger(Context),
    ok.

log_locations() ->
    Handlers = logger:get_handler_config(),
    log_locations(Handlers, []).

log_locations([#{module := logger_std_h,
                 config := #{type := file,
                             file := Filename}} | Rest],
              Locations) ->
    Locations1 = add_once(Locations, Filename),
    log_locations(Rest, Locations1);
log_locations([#{module := logger_std_h,
                 config := #{type := standard_io}} | Rest],
              Locations) ->
    Locations1 = add_once(Locations, "<stdout>"),
    log_locations(Rest, Locations1);
log_locations([#{module := logger_std_h,
                 config := #{type := standard_error}} | Rest],
              Locations) ->
    Locations1 = add_once(Locations, "<stderr>"),
    log_locations(Rest, Locations1);
log_locations([_ | Rest], Locations) ->
    log_locations(Rest, Locations);
log_locations([], Locations) ->
    lists:sort(Locations).

add_once(Locations, Location) ->
    case lists:member(Location, Locations) of
        false -> [Location | Locations];
        true  -> Locations
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

get_log_base_dir(#{log_base_dir := LogBaseDirFromEnv} = Context) ->
    case rabbit_env:has_var_been_overridden(Context, log_base_dir) of
        false -> application:get_env(lager, log_root, LogBaseDirFromEnv);
        true  -> LogBaseDirFromEnv
    end.

%% -------------------------------------------------------------------
%% Logger's handlers configuration.
%% -------------------------------------------------------------------

configure_logger(Context) ->
    %% Configure main handlers.
    %% We distinguish them by their type and possibly other
    %% parameters (file name, syslog settings, etc.).
    LogConfig0 = get_log_configuration_from_app_env(),
    LogConfig1 = handle_default_and_overridden_outputs(LogConfig0, Context),
    LogConfig2 = apply_log_levels_from_env(LogConfig1, Context),
    LogConfig3 = make_filenames_absolute(LogConfig2, Context),
    LogConfig4 = configure_formatters(LogConfig3, Context),
    Handlers = create_logger_handlers_conf(LogConfig4),
    ?LOG_DEBUG("Logger handlers:~n  ~p", [Handlers],
               #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_NOTICE("Logging: switching to configured handler(s); following "
                "messages may not be visible on <stdout>",
                #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ok = install_handlers(Handlers),
    ?LOG_NOTICE("Logging: configured log handlers are now ACTIVE",
                #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ok = maybe_log_test_messsages(LogConfig3),
    ok.

get_log_configuration_from_app_env() ->
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
    #{global => GlobalConfig,
      per_category => PerCatConfig}.

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
    normalize_main_file_output(
      Props,
      #{module => logger_std_h,
        config => #{type => file}},
      Outputs);
normalize_main_output(console, Props, Outputs) ->
    normalize_main_console_output(
      Props,
      #{module => logger_std_h,
        config => #{type => standard_io}},
      Outputs).
%% TODO: Normalize other output types.

normalize_main_file_output([{file, false} | _], _, Outputs) ->
    maps:filter(
      fun
          (_, #{module := logger_std_h,
                config := #{type := file}}) -> false;
          (_, _)                            -> true
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
          (_, #{module := logger_std_h,
                config := #{type := standard_io}})    -> false;
          (_, #{module := logger_std_h,
                config := #{type := standard_error}}) -> false;
          (_, _)                                      -> true
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
               config => #{type => file,
                           file => Filename}},
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
                          config => #{type => standard_io}}];
                   Filename when NoOutputsConfigured orelse Overridden ->
                       [#{module => logger_std_h,
                          config => #{type => file,
                                      file => Filename}}];
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
                          config => #{type => file,
                                      file => Filename}}];
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
  #{global := GlobalConfig, per_category := PerCatConfig} = LogConfig,
  Context) ->
    LogBaseDir = get_log_base_dir(Context),
    GlobalConfig1 = make_filenames_absolute1(GlobalConfig, LogBaseDir),
    PerCatConfig1 = maps:map(
                      fun(_, CatConfig) ->
                              make_filenames_absolute1(CatConfig, LogBaseDir)
                      end, PerCatConfig),
    LogConfig#{global => GlobalConfig1, per_category => PerCatConfig1}.

make_filenames_absolute1(#{outputs := Outputs} = Config, LogBaseDir) ->
    Outputs1 = lists:map(
                 fun
                     (#{module := logger_std_h,
                        config := #{type := file,
                                    file := Filename} = Cfg} = Output) ->
                         Cfg1 = Cfg#{file => filename:absname(
                                               Filename, LogBaseDir)},
                         Output#{config => Cfg1};
                     (Output) ->
                         Output
                 end, Outputs),
    Config#{outputs => Outputs1}.

configure_formatters(
  #{global := GlobalConfig, per_category := PerCatConfig} = LogConfig,
  Context) ->
    GlobalConfig1 = configure_formatters1(GlobalConfig, Context),
    PerCatConfig1 = maps:map(
                      fun(_, CatConfig) ->
                              configure_formatters1(CatConfig, Context)
                      end, PerCatConfig),
    LogConfig#{global => GlobalConfig1, per_category => PerCatConfig1}.

configure_formatters1(#{outputs := Outputs} = Config, Context) ->
    StdioFormatter = rabbit_prelaunch_early_logging:default_formatter(
                       Context),
    FileFormatter = rabbit_prelaunch_early_logging:default_formatter(
                      Context#{output_supports_colors => false}),
    Outputs1 = lists:map(
                 fun
                     (#{module := logger_std_h,
                        config := #{type := standard_io}} = Output) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => StdioFormatter}
                         end;
                     (Output) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => FileFormatter}
                         end
                 end, Outputs),
    Config#{outputs => Outputs1}.

create_logger_handlers_conf(
  #{global := GlobalConfig, per_category := PerCatConfig}) ->
    Handlers0 = create_global_handlers_conf(GlobalConfig),
    Handlers1 = create_per_cat_handlers_conf(PerCatConfig, Handlers0),
    Handlers2 = adjust_log_levels(Handlers1),
    Handlers3 = assign_handler_ids(Handlers2),
    Handlers3.

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
  #{module := logger_std_h, config := #{type := file, file := Filename}}) ->
    {file, Filename};
create_handler_key(
  #{module := logger_std_h, config := #{type := standard_io}}) ->
    {console, standard_io}.

create_handler_conf(Output, global, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{level => Level,
            filter_default => log,
            filters => []};
create_handler_conf(Output, CatName, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{level => Level,
            filter_default => stop,
            filters => [{CatName,
                         {fun filter_cat_event/2, {CatName, Level}}}]}.

update_handler_conf(
  #{level := ConfiguredLevel} = Handler, global, Output) ->
    case Output of
        #{level := NewLevel} ->
            Handler#{level =>
                     get_less_severe_level(NewLevel, ConfiguredLevel)};
        _ ->
            Handler
    end;
update_handler_conf(Handler, CatName, Output) ->
    Handler1 = add_global_filter(Handler),
    add_cat_filter(Handler1, CatName, Output).

compute_level_from_config_and_output(Config, Output) ->
    GeneralLevel = maps:get(level, Config, ?DEFAULT_LOG_LEVEL),
    OutputLevel = maps:get(level, Output, GeneralLevel),
    get_less_severe_level(OutputLevel, GeneralLevel).

filter_cat_in_global_handlers(Handlers, CatName, CatConfig) ->
    maps:map(
      fun(_, Handler) ->
              Handler1 = add_global_filter(Handler),
              add_cat_filter(Handler1, CatName, CatConfig)
      end, Handlers).

filter_out_cat_in_other_handlers(Handlers, CatName) ->
    maps:map(
      fun(_, #{filters := Filters} = Handler) ->
              case lists:keymember(CatName, 1, Filters) of
                  true  -> Handler;
                  false -> add_cat_filter(Handler, CatName, none)
              end
      end, Handlers).

add_global_filter(#{level := Level, filters := []} = Handler) ->
    %% This is a global handler: we need to add a filter for global & unknown
    %% events because categories using the same handler might use a lower
    %% level.
    Filters = [{global, {fun filter_global_event/2, Level}}],
    Handler#{filters => Filters};
add_global_filter(Handler) ->
    Handler.

add_cat_filter(#{filters := Filters} = Handler, CatName, none = Level) ->
    Filters1 = [{CatName, {fun filter_cat_event/2, {CatName, Level}}}
                | Filters],
    Handler#{filters => Filters1};
add_cat_filter(#{filters := Filters} = Handler, CatName, CatConfig) ->
    Level = compute_level_from_config_and_output(Handler, CatConfig),
    Filters1 = [{CatName, {fun filter_cat_event/2, {CatName, Level}}}
                | Filters],
    Handler#{filters => Filters1}.

filter_global_event(
  #{meta := #{domain := ?LOGGER_DOMAIN_GLOBAL}},
  none) ->
    stop;
filter_global_event(
  #{level := Level,
    meta := #{domain := ?LOGGER_DOMAIN_GLOBAL}} = LogEvent,
  GlobalLevel) ->
    case logger:compare_levels(Level, GlobalLevel) of
        lt -> stop;
        _  -> LogEvent
    end;
filter_global_event(LogEvent, _) ->
    LogEvent.

filter_cat_event(
  #{meta := #{domain := [?LOGGER_SUPER_DOMAIN_NAME, CatName | _]}},
  {CatName, none}) ->
    stop;
filter_cat_event(
  #{level := Level,
    meta := #{domain := [?LOGGER_SUPER_DOMAIN_NAME, CatName | _]}} = LogEvent,
  {CatName, CatLevel}) ->
    case logger:compare_levels(Level, CatLevel) of
        lt -> stop;
        _  -> LogEvent
    end;
filter_cat_event(_, _) ->
    ignore.

adjust_log_levels(Handlers) ->
    maps:map(
      fun(_, #{level := GeneralLevel, filters := Filters} = Handler) ->
              Level = lists:foldl(
                        fun
                            ({_, {_, {_, LvlA}}}, LvlB) ->
                                get_less_severe_level(LvlA, LvlB);
                            ({_, {_, LvlA}}, LvlB) ->
                                get_less_severe_level(LvlA, LvlB)
                        end, GeneralLevel, Filters),
              Handler#{level => Level}
      end, Handlers).

assign_handler_ids(Handlers) ->
    Handlers1 = [maps:get(Key, Handlers)
                 || Key <- lists:sort(maps:keys(Handlers))],
    assign_handler_ids(Handlers1, #{next_file => 1}, []).

assign_handler_ids(
  [#{module := logger_std_h, config := #{type := file}} = Handler | Rest],
  #{next_file := NextFile} = State,
  Result) ->
    Id = list_to_atom(rabbit_misc:format("rabbitmq_log_file_~b", [NextFile])),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(
      Rest, State#{next_file => NextFile + 1}, [Handler1 | Result]);
assign_handler_ids(
  [#{module := logger_std_h, config := #{type := standard_io}} = Handler | Rest],
  State,
  Result) ->
    Handler1 = Handler#{id => stdout},
    assign_handler_ids(
      Rest, State, [Handler1 | Result]);
assign_handler_ids([], _, Result) ->
    lists:reverse(Result).

install_handlers([]) ->
    throw(no_logger_handler_configured);
install_handlers(Handlers) ->
    ok = rabbit_prelaunch_early_logging:reset_early_setup(),
    ok = do_install_handlers(Handlers),
    ok = logger:remove_handler(default),
    ok = define_primary_level(Handlers),
    ok.

do_install_handlers([#{id := Id, module := Module} = Handler | Rest]) ->
    ok = logger:add_handler(Id, Module, Handler),
    do_install_handlers(Rest);
do_install_handlers([]) ->
    ok.

define_primary_level(Handlers) ->
    define_primary_level(Handlers, emergency).

define_primary_level([#{level := Level} | Rest], PrimaryLevel) ->
    NewLevel = get_less_severe_level(Level, PrimaryLevel),
    define_primary_level(Rest, NewLevel);
define_primary_level([], PrimaryLevel) ->
    logger:set_primary_config(level, PrimaryLevel).

get_less_severe_level(LevelA, LevelB) ->
    case logger:compare_levels(LevelA, LevelB) of
        lt -> LevelA;
        _  -> LevelB
    end.

maybe_log_test_messsages(
  #{per_category := #{prelaunch := #{level := debug}}}) ->
    log_test_messages();
maybe_log_test_messsages(
  #{global := #{level := debug}}) ->
    log_test_messages();
maybe_log_test_messsages(_) ->
    ok.

log_test_messages() ->
    ?LOG_DEBUG("Testing debug log level",
               #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_INFO("Testing info log level",
              #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_NOTICE("Testing notice log level",
                #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_WARNING("Testing warning log level",
                 #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_ERROR("Testing error log level",
               #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_CRITICAL("Testing critical log level",
                  #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_ALERT("Testing alert log level",
               #{domain => ?LOGGER_DOMAIN_PRELAUNCH}),
    ?LOG_EMERGENCY("Testing emergency log level",
                   #{domain => ?LOGGER_DOMAIN_PRELAUNCH}).
