%%%-------------------------------------------------------------------
%%% @author mutiashami
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jul 2018 14:09
%%%-------------------------------------------------------------------
-module(riak_kv_get_helper).
-author("mutiashami").
-behaviour(riak_core_vnode).
-include_lib("riak_core_pb.hrl").
-include("riak_kv_types.hrl").
-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_index.hrl").
%% API
-export([init/1,get/3]).
-export([start_vnode/1,
  start_vnodes/1,
  terminate/2,
  handle_overload_command/3,
  handle_command/3,
  delete/1,
  handle_handoff_command/3,
  encode_handoff_item/2,
  handle_coverage/4,
  handle_exit/3,
  handle_handoff_data/2,
  handoff_cancelled/1,
  handoff_finished/2,
  handoff_started/2,
  handoff_starting/2,
  is_empty/1,
  do_get/4,
  forward_get/3,
  handle_info/2,
  start_link/0]).

-define(DEFAULT_CNTR_LEASE_ERRS, 20).
-define(DEFAULT_CNTR_LEASE_TO, 20000).
-define(INDEX(A,B,C), _=element(1,{{_A1, _A2} = A,B,C}), ok).
-define(IS_SEARCH_ENABLED_FOR_BUCKET(BProps), _=element(1, {BProps}), false).
-define(MD_CACHE_BASE, "riak_kv_vnode_md_cache").
-record(counter_state, {
  %% kill switch, if for any reason one wants disable per-key-epoch, then set
  %% [{riak_kv, [{per_key_epoch, false}]}].
  use = true :: boolean(),
  %% The number of new epoch writes co-ordinated by this vnode
  %% What even is a "key epoch?" It is any time a key is
  %% (re)created. A new write, a write not yet coordinated by
  %% this vnode, a write where local state is unreadable.
  cnt = 0 :: non_neg_integer(),
  %% Counter leased up-to. For totally new state/id
  %% this will be that flush threshold See config value
  %% `{riak_kv, counter_lease_size}'
  lease = 0 :: non_neg_integer(),
  lease_size = 0 :: non_neg_integer(),
  %% Has a lease been requested but not granted yet
  leasing = false :: boolean()
}).
-record(putargs, {returnbody :: boolean(),
  coord:: boolean(),
  lww :: boolean(),
  bkey :: {binary(), binary()},
  robj :: term(),
  index_specs=[] :: [{index_op(), binary(), index_value()}],
  reqid :: non_neg_integer(),
  bprops :: riak_kv_bucket:props(),
  starttime :: non_neg_integer(),
  prunetime :: undefined| non_neg_integer(),
  readrepair=false :: boolean(),
  is_index=false :: boolean(), %% set if the b/end supports indexes
  crdt_op = undefined :: undefined | term() %% if set this is a crdt operation
}).
-record(state, {idx :: partition(),
  mod :: module(),
  async_put :: boolean(),
  modstate :: term(),
  mrjobs :: term(),
  vnodeid :: undefined | binary(),
  delete_mode :: keep | immediate | pos_integer(),
  bucket_buf_size :: pos_integer(),
  index_buf_size :: pos_integer(),
  key_buf_size :: pos_integer(),
  async_folding :: boolean(),
  in_handoff = false :: boolean(),
  handoff_target :: node(),
  handoffs_rejected = 0 :: integer(),
  forward :: node() | [{integer(), node()}],
  hashtrees :: pid(),
  upgrade_hashtree = false :: boolean(),
  md_cache :: ets:tab(),
  md_cache_size :: pos_integer(),
  counter :: #counter_state{},
  status_mgr_pid :: pid() %% a process that manages vnode status persistence
}).
-define(MAX_CNTR_LEASE, 50000000).
-define(DEFAULT_CNTR_LEASE, 10000).
-define(ELSE, true).
-define(DEFAULT_HASHTREE_TOKENS, 90).
-ifdef(TEST).
-define(YZ_SHOULD_HANDOFF(X), true).
-else.
-define(YZ_SHOULD_HANDOFF(X), yz_kv:should_handoff(X)).
-endif.
-type index_op() :: add | remove.
-type index_value() :: integer() | binary().
%%-type index() :: non_neg_integer().
-type state() :: #state{}.
-include_lib("bitcask/include/bitcask.hrl").
%%-type vnodeid() :: binary().
%%-type counter_lease_error() :: {error, counter_lease_max_errors | counter_lease_timeout}.

start_link() ->
  error_logger:info_msg("helper  inside  get helper  start link "),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, riak_kv_get_helper).

start_vnodes(IdxList) ->
    riak_core_vnode_master:get_vnode_pid(IdxList, riak_kv_get_helper).
init([Index]) ->
  error_logger:info_msg("helper  inside  get helper  init"),
  Mod = app_helper:get_env(riak_kv, storage_backend),
  Configuration = app_helper:get_env(riak_kv),
  BucketBufSize = app_helper:get_env(riak_kv, bucket_buffer_size, 1000),
  IndexBufSize = app_helper:get_env(riak_kv, index_buffer_size, 100),
  KeyBufSize = app_helper:get_env(riak_kv, key_buffer_size, 100),
  WorkerPoolSize = app_helper:get_env(riak_kv, worker_pool_size, 10),
  UseEpochCounter = app_helper:get_env(riak_kv, use_epoch_counter, true),
  %%  This _has_ to be a non_neg_integer(), and really, if it is
  %%  zero, you are fsyncing every.single.key epoch.
  CounterLeaseSize = min(?MAX_CNTR_LEASE,
    non_neg_env(riak_kv, counter_lease_size, ?DEFAULT_CNTR_LEASE)),
  {ok, StatusMgr} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),
  {ok, {VId, CounterState}} = get_vnodeid_and_counter(StatusMgr, CounterLeaseSize, UseEpochCounter),
  DeleteMode = app_helper:get_env(riak_kv, delete_mode, 3000),
  AsyncFolding = app_helper:get_env(riak_kv, async_folds, true) == true,
  MDCacheSize = app_helper:get_env(riak_kv, vnode_md_cache_size),
  MDCache =
    case MDCacheSize of
      N when is_integer(N),
        N > 0 ->
        lager:debug("Initializing metadata cache with size limit: ~p bytes",
          [MDCacheSize]),
        new_md_cache(VId);
      _ ->
        lager:debug("No metadata cache size defined, not starting"),
        undefined
    end,
  case catch Mod:start(Index, Configuration) of
    {ok, ModState} ->
      %% Get the backend capabilities
      DoAsyncPut =  case app_helper:get_env(riak_kv, allow_async_put, true) of
                      true ->
                        erlang:function_exported(Mod, async_put, 5);
                      _ ->
                        false
                    end,
      State = #state{idx=Index,
        async_folding=AsyncFolding,
        mod=Mod,
        async_put = DoAsyncPut,
        modstate=ModState,
        vnodeid=VId,
        counter=CounterState,
        status_mgr_pid=StatusMgr,
        delete_mode=DeleteMode,
        bucket_buf_size=BucketBufSize,
        index_buf_size=IndexBufSize,
        key_buf_size=KeyBufSize,
        mrjobs=dict:new(),
        md_cache=MDCache,
        md_cache_size=MDCacheSize},
      try_set_vnode_lock_limit(Index),
      case AsyncFolding of
        true ->
          %% Create worker pool initialization tuple
          FoldWorkerPool = {pool, riak_kv_worker, WorkerPoolSize, []},
          State2 = maybe_create_hashtrees(State),
          {ok, State2, [FoldWorkerPool]};
        false ->
          {ok, State}
      end;
    {error, Reason} ->
      lager:error("Failed to start ~p backend for index ~p error: ~p",
        [Mod, Index, Reason]),
      riak:stop("backend module failed to start."),
      {error, Reason};
    {'EXIT', Reason1} ->
      lager:error("Failed to start ~p backend for index ~p crash: ~p",
        [Mod, Index, Reason1]),
      riak:stop("backend module failed to start."),
      {error, Reason1}
  end.
get(Preflist, BKey, ReqId) ->
  error_logger:info_msg("helper  well inside  get 3 kv  vnode"),
  %% Assuming this function is called from a FSM process
  %% so self() == FSM pid
  get(Preflist, BKey, ReqId, {fsm, undefined, self()}).

get(Preflist, BKey, ReqId, Sender) ->
  error_logger:info_msg("helper well inside  get 4 kv  vnode  Sender ~p",[Sender]),
  Req = ?KV_GET_REQ{bkey=sanitize_bkey(BKey), req_id=ReqId},
%%  riak_core_vnode_master:command(Preflist, Req, Sender, riak_kv_vnode_master).
  riak_core_vnode_master:command(Preflist, Req, Sender, riak_kv_get_helper_master).
sanitize_bkey({{<<"default">>, B}, K}) ->
  {B, K};
sanitize_bkey(BKey) ->
  BKey.

handle_overload_command(?KV_GET_REQ{req_id=ReqID}, Sender, Idx) ->
  riak_core_vnode:reply(Sender, {r, {error, overload}, Idx, ReqID}).
%%
handle_command(?KV_GET_REQ{bkey=BKey,req_id=ReqId},Sender,State) ->
  error_logger:info_msg("helper  inside  get helper  handle command"),
do_get(Sender, BKey, ReqId, State).
do_get(_Sender, BKey, ReqID,
    State=#state{idx=Idx, mod=Mod, modstate=ModState}) ->
  error_logger:info_msg("helper  inside  do  get  Mod ~p",[Mod]),
  StartTS = os:timestamp(),
  {Retval, ModState1} = do_get_term(BKey, Mod, ModState),
  case Retval of
    {ok, Obj} ->
      maybe_cache_object(BKey, Obj, State);
    _ ->
      ok
  end,
  update_vnode_stats(vnode_get, Idx, StartTS),
  {reply, {r, Retval, Idx, ReqID}, State#state{modstate=ModState1}}.

do_get_term({Bucket, Key}, Mod, ModState) ->
  case do_get_object(Bucket, Key, Mod, ModState) of
    {ok, Obj, UpdModState} ->
      {{ok, Obj}, UpdModState};
    %% @TODO Eventually it would be good to
    %% make the use of not_found or notfound
    %% consistent throughout the code.
    {error, not_found, UpdModState} ->
      {{error, notfound}, UpdModState};
    {error, Reason, UpdModState} ->
      {{error, Reason}, UpdModState};
    Err ->
      Err
  end.

update_vnode_stats(Op, Idx, StartTS) ->
  ok = riak_kv_stat:update({Op, Idx, timer:now_diff( os:timestamp(), StartTS)}).
maybe_cache_object(BKey, Obj, #state{md_cache = MDCache,
  md_cache_size = MDCacheSize}) ->
  case MDCache of
    undefined ->
      ok;
    _ ->
      VClock = riak_object:vclock(Obj),
      IndexData = riak_object:index_data(Obj),
      insert_md_cache(MDCache, MDCacheSize, BKey, {VClock, IndexData})
  end.
insert_md_cache(Table, MaxSize, BKey, MD) ->
  TS = os:timestamp(),
  case ets:insert(Table, {TS, BKey, MD}) of
    true ->
      Size = ets:info(Table, memory),
      case Size > MaxSize of
        true ->
          trim_md_cache(Table, MaxSize);
        false ->
          ok
      end
  end.

trim_md_cache(Table, MaxSize) ->
  Oldest = ets:first(Table),
  case Oldest of
    '$end_of_table' ->
      ok;
    BKey ->
      ets:delete(Table, BKey),
      Size = ets:info(Table, memory),
      case Size > MaxSize of
        true ->
          trim_md_cache(Table, MaxSize);
        false ->
          ok
      end
  end.

do_get_object(Bucket, Key, Mod, ModState) ->
  case uses_r_object(Mod, ModState, Bucket) of
    true ->
      %% Non binary returns do not trigger size warnings
      error_logger:info_msg(" helper  true  do  get  object Mod ~p ",[Mod]),
      Mod:get_object(Bucket, Key, false, ModState);
    false ->
      case do_get_binary(Bucket, Key, Mod, ModState) of
        {ok, ObjBin, _UpdModState} ->
          BinSize = size(ObjBin),
          WarnSize = app_helper:get_env(riak_kv, warn_object_size),
          case BinSize > WarnSize of
            true ->
              lager:warning("Read large object ~p/~p (~p bytes)",
                [Bucket, Key, BinSize]);
            false ->
              ok
          end,
          try
            case riak_object:from_binary(Bucket, Key, ObjBin) of
              {error, Reason} ->
                throw(Reason);
              RObj ->
                {ok, RObj, _UpdModState}
            end
          catch _:_ ->
            lager:warning("Unreadable object ~p/~p discarded",
              [Bucket,Key]),
            {error, not_found, _UpdModState}
          end;
        Else ->
          Else
      end
  end.
do_get_binary(Bucket, Key, Mod, ModState) ->
  case uses_r_object(Mod, ModState, Bucket) of
    true ->
      error_logger:info_msg(" helper  true  Mod ~p ",[Mod]),
      Mod:get_object(Bucket, Key, true, ModState);
    false ->
      error_logger:info_msg(" helper  false Mod ~p ",[Mod]),
      Mod:get(Bucket, Key, ModState)
  end.
uses_r_object(Mod, ModState, Bucket) ->
  {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
  lists:member(uses_r_object, Capabilities).
%% -callbacks
handle_handoff_command(Req=?KV_PUT_REQ{}, Sender, State) ->
  ?KV_PUT_REQ{options=Options} = Req,
  case proplists:get_value(coord, Options, false) of
    false ->
      {noreply, NewState} = handle_command(Req, Sender, State),
      {forward, NewState};
    true ->
      %% riak_kv#1046 - don't make fake siblings. Perform the
      %% put, and create a new request to forward on, that
      %% contains the frontier, much like the value returned to
      %% a put fsm, then replicated.
      #state{idx=Idx} = State,
      ?KV_PUT_REQ{bkey=BKey,
        object=Object,
        req_id=ReqId,
        start_time=StartTime,
        options=Options} = Req,
      StartTS = os:timestamp(),
      riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
      {Reply, UpdState} = do_put(Sender, BKey,  Object, ReqId, StartTime, Options, State),
      update_vnode_stats(vnode_put, Idx, StartTS),

      case Reply of
        %%  NOTE: Coord is always `returnbody` as a put arg
        {dw, Idx, NewObj, ReqId} ->
          %% DO NOT coordinate again at the next owner!
          NewReq = Req?KV_PUT_REQ{options=proplists:delete(coord, Options),
            object=NewObj},
          {forward, NewReq, UpdState};
        _Error ->
          %% Don't forward a failed attempt to put, as you
          %% need the successful object
          {noreply, UpdState}
      end
  end;
handle_handoff_command(?KV_W1C_PUT_REQ{}=Request, Sender, State) ->
  NewState0 = case handle_command(Request, Sender, State) of
                {noreply, NewState} ->
                  NewState;
                {reply, Reply, NewState} ->
                  %% reply directly to the sender, as we will be forwarding the
                  %% the request on to the handoff node.
                  riak_core_vnode:reply(Sender, Reply),
                  NewState
              end,
  {forward, NewState0};

%% Handle all unspecified cases locally without forwarding
handle_handoff_command(Req, Sender, State) ->
  handle_command(Req, Sender, State).
terminate(_Reason, #state{idx=Idx, mod=Mod, modstate=ModState,hashtrees=Trees}) ->
  Mod:stop(ModState),

  %% Explicitly stop the hashtree rather than relying on the process monitor
  %% to detect the vnode exit.  As riak_kv_index_hashtree is not a supervised
  %% process in the riak_kv application, on graceful shutdown riak_kv and
  %% riak_core can complete their shutdown before the hashtree is written
  %% to disk causing the hashtree to be closed dirty.
  riak_kv_index_hashtree:sync_stop(Trees),
  riak_kv_stat:unregister_vnode_stats(Idx),
  ok.

delete(State=#state{status_mgr_pid=StatusMgr, mod=Mod, modstate=ModState}) ->
  %% clear vnodeid first, if drop removes data but fails
  %% want to err on the side of creating a new vnodeid
  {ok, cleared} = clear_vnodeid(StatusMgr),
  UpdModState = case Mod:drop(ModState) of
                  {ok, S} ->
                    S;
                  {error, Reason, S2} ->
                    lager:error("Failed to drop ~p. Reason: ~p~n", [Mod, Reason]),
                    S2
                end,
  case State#state.hashtrees of
    undefined ->
      ok;
    HT ->
      riak_kv_index_hashtree:destroy(HT)
  end,
  {ok, State#state{modstate=UpdModState,vnodeid=undefined,hashtrees=undefined}}.

encode_handoff_item({B, K}, V) ->
  %% before sending data to another node change binary version
  %% to one supported by the cluster. This way we don't send
  %% unsupported formats to old nodes
  ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
  try
    Value  = riak_object:to_binary_version(ObjFmt, B, K, V),
    encode_binary_object(B, K, Value)
  catch Error:Reason ->
    lager:warning("Handoff encode failed: ~p:~p",
      [Error,Reason]),
    corrupted
  end.
handle_coverage(?KV_LISTBUCKETS_REQ{item_filter=ItemFilter},
                _FilterVNodes,
                Sender,
                State=#state{async_folding=AsyncFolding,
                             bucket_buf_size=BufferSize,
                             mod=Mod,
                             modstate=ModState}) ->
    %% Construct the filter function
    Filter = riak_kv_coverage_filter:build_filter(ItemFilter),
    BufferMod = riak_kv_fold_buffer,

    Buffer = BufferMod:new(BufferSize, result_fun(Sender)),
    FoldFun = fold_fun(buckets, BufferMod, Filter, undefined),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    case AsyncFolding andalso AsyncBackend of
        true ->
            Opts = [async_fold];
        false ->
            Opts = []
    end,
    case list(FoldFun, FinishFun, Mod, fold_buckets, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
    end;
handle_coverage(#riak_kv_listkeys_req_v3{bucket=Bucket,
                                         item_filter=ItemFilter},
                FilterVNodes, Sender, State) ->
    %% v3 == no backpressure
    ResultFun = result_fun(Bucket, Sender),
    Opts = [{bucket, Bucket}],
    handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State);
handle_coverage(?KV_LISTKEYS_REQ{bucket=Bucket,
                                 item_filter=ItemFilter},
                FilterVNodes, Sender, State) ->
    %% v4 == ack-based backpressure
    ResultFun = result_fun_ack(Bucket, Sender),
    Opts = [{bucket, Bucket}],
    handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State);
handle_coverage(#riak_kv_index_req_v1{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes, Sender, State) ->
    %% v1 == no backpressure
    handle_coverage_index(Bucket, ItemFilter, Query,
                          FilterVNodes, Sender, State, fun result_fun/2);
handle_coverage(?KV_INDEX_REQ{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes, Sender, State) ->
    %% v2 = ack-based backpressure
    handle_coverage_index(Bucket, ItemFilter, Query,
                          FilterVNodes, Sender, State, fun result_fun_ack/2).
handle_exit(Pid, Reason, State=#state{status_mgr_pid=Pid, idx=Index, counter=CntrState}) ->
  lager:error("Vnode status manager exit ~p", [Reason]),
  %% The status manager died, start a new one
  #counter_state{lease_size=LeaseSize, leasing=Leasing, use=UseEpochCounter} = CntrState,
  {ok, NewPid} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),

  if Leasing ->
    %% Crashed when getting a lease, try again pathalogical
    %% bad case here is that the lease gets to disk and the
    %% manager crashes, meaning an ever growing lease/new ids
    ok = riak_kv_vnode_status_mgr:lease_counter(NewPid, LeaseSize);
    ?ELSE ->
      ok
  end,
  {noreply, State#state{status_mgr_pid=NewPid}};
handle_exit(_Pid, Reason, State) ->
  %% A linked processes has died so the vnode
  %% process should take appropriate action here.
  %% The default behavior is to crash the vnode
  %% process so that it can be respawned
  %% by riak_core_vnode_master to prevent
  %% messages from stacking up on the process message
  %% queue and never being processed.
  lager:error("Linked process exited. Reason: ~p", [Reason]),
  {stop, linked_process_crash, State}.

handle_handoff_data(BinObj, State) ->
    try
        {BKey, Val} = decode_binary_object(BinObj),
        {B, K} = BKey,
        case do_diffobj_put(BKey, riak_object:from_binary(B, K, Val),
                            State) of
            {ok, UpdModState} ->
                {reply, ok, State#state{modstate=UpdModState}};
            {error, Reason, UpdModState} ->
                {reply, {error, Reason}, State#state{modstate=UpdModState}}
        end
    catch Error:Reason2 ->
            lager:warning("Unreadable object discarded in handoff: ~p:~p",
                          [Error, Reason2]),
            {reply, ok, State}
    end.
handoff_cancelled(State) ->
    {ok, State#state{in_handoff=false, handoff_target=undefined}}.
handoff_started(SrcPartition, WorkerPid) ->
    case maybe_get_vnode_lock(SrcPartition, WorkerPid) of
        ok ->
            FoldOpts = [{iterator_refresh, true}],
            {ok, FoldOpts};
        max_concurrency -> {error, max_concurrency}
    end.
handoff_finished(_TargetNode, State) ->
    {ok, State#state{in_handoff=false, handoff_target=undefined}}.
handoff_starting({_HOType, TargetNode}=_X, State=#state{handoffs_rejected=RejectCount}) ->
  MaxRejects = app_helper:get_env(riak_kv, handoff_rejected_max, 6),
  case MaxRejects =< RejectCount orelse ?YZ_SHOULD_HANDOFF(_X) of
    true ->
      {true, State#state{in_handoff=true, handoff_target=TargetNode}};
    false ->
      {false, State#state{in_handoff=false, handoff_target=undefined, handoffs_rejected=RejectCount + 1 }}
  end.
is_empty(State=#state{mod=Mod, modstate=ModState}) ->
    IsEmpty = Mod:is_empty(ModState),
    case IsEmpty of
        true ->
            {true, State};
        false ->
            Size = maybe_calc_handoff_size(State),
            {false, Size, State}
    end.

non_neg_env(App, EnvVar, Default) when is_integer(Default),
                                       Default > 0 ->
    case app_helper:get_env(App, EnvVar, Default) of
        N when is_integer(N),
               N > 0 ->
            N;
        X ->
            lager:warning("Non-integer/Negative integer ~p for vnode counter config ~p."
                          " Using default ~p",
                          [X, EnvVar, Default]),
            Default
    end.

get_vnodeid_and_counter(StatusMgr, CounterLeaseSize, UseEpochCounter) ->
    {ok, {VId, Counter, Lease}} = riak_kv_vnode_status_mgr:get_vnodeid_and_counter(StatusMgr, CounterLeaseSize),
    {ok, {VId, #counter_state{cnt=Counter, lease=Lease, lease_size=CounterLeaseSize, use=UseEpochCounter}}}.

new_md_cache(VId) ->
  MDCacheName = list_to_atom(?MD_CACHE_BASE ++ integer_to_list(binary:decode_unsigned(VId))),
  %% ordered set to make sure that the first key is the oldest
  %% term format is {TimeStamp, Key, ValueTuple}
  ets:new(MDCacheName, [ordered_set, {keypos,2}]).
try_set_vnode_lock_limit(Idx) ->
    %% By default, register per-vnode concurrency limit "lock" with 1 so that only a
    %% single participating subsystem can run a vnode fold at a time. Participation is
    %% voluntary :-)
    Concurrency = case app_helper:get_env(riak_kv, vnode_lock_concurrency, 1) of
                      N when is_integer(N) -> N;
                      _NotNumber -> 1
                  end,
    try_set_concurrency_limit(?KV_VNODE_LOCK(Idx), Concurrency).

try_set_concurrency_limit(Lock, Limit) ->
    try_set_concurrency_limit(Lock, Limit, riak_core_bg_manager:use_bg_mgr()).

try_set_concurrency_limit(_Lock, _Limit, false) ->
    %% skip background manager
    ok;
try_set_concurrency_limit(Lock, Limit, true) ->
    %% this is ok to do more than once
    case riak_core_bg_manager:set_concurrency_limit(Lock, Limit) of
        unregistered ->
            %% not ready yet, try again later
            lager:debug("Background manager unavailable. Will try to set: ~p later.", [Lock]),
            erlang:send_after(250, ?MODULE, {set_concurrency_limit, Lock, Limit});
        _ ->
            lager:debug("Registered lock: ~p", [Lock]),
            ok
    end.
handle_coverage_keyfold(Bucket, ItemFilter, Query,
    FilterVNodes, Sender, State,
    ResultFunFun) ->
  handle_coverage_fold(fold_keys, Bucket, ItemFilter, Query,
    FilterVNodes, Sender, State, ResultFunFun).
handle_coverage_fold(FoldType, Bucket, ItemFilter, ResultFun,
                        FilterVNodes, Sender, Opts0,
                        State=#state{async_folding=AsyncFolding,
                                     idx=Index,
                                     key_buf_size=DefaultBufSz,
                                     mod=Mod,
                                     modstate=ModState}) ->
    %% Construct the filter function
    FilterVNode = proplists:get_value(Index, FilterVNodes),
    Filter = riak_kv_coverage_filter:build_filter(Bucket, ItemFilter, FilterVNode),
    BufferMod = riak_kv_fold_buffer,
    BufferSize = proplists:get_value(buffer_size, Opts0, DefaultBufSz),
    Buffer = BufferMod:new(BufferSize, ResultFun),
    Extras = fold_extras_keys(Index, Bucket),
    FoldFun = fold_fun(keys, BufferMod, Filter, Extras),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    Opts = case AsyncFolding andalso AsyncBackend of
               true ->
                   [async_fold | Opts0];
               false ->
                   Opts0
           end,
    case list(FoldFun, FinishFun, Mod, FoldType, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
    end.
maybe_create_hashtrees(State) ->
  maybe_create_hashtrees(riak_kv_entropy_manager:enabled(), State).

-spec maybe_create_hashtrees(boolean(), state()) -> state().
maybe_create_hashtrees(false, State) ->
  State#state{upgrade_hashtree=false};
maybe_create_hashtrees(true, State=#state{idx=Index, upgrade_hashtree=Upgrade,
  mod=Mod, modstate=ModState}) ->
  %% Only maintain a hashtree if a primary vnode
  {ok, Ring} = riak_core_ring_manager:get_my_ring(),
  case riak_core_ring:vnode_type(Ring, Index) of
    primary ->
      {ok, ModCaps} = Mod:capabilities(ModState),
      Empty = case is_empty(State) of
                {true, _}     -> true;
                {false, _, _} -> false
              end,
      Opts = [use_2i || lists:member(indexes, ModCaps)]
        ++ [vnode_empty || Empty]
        ++ [upgrade || Upgrade],
      case riak_kv_index_hashtree:start(Index, self(), Opts) of
        {ok, Trees} ->
          monitor(process, Trees),
          State#state{hashtrees=Trees, upgrade_hashtree=false};
        Error ->
          lager:info("riak_kv/~p: unable to start index_hashtree: ~p",
            [Index, Error]),
          erlang:send_after(1000, self(), retry_create_hashtree),
          State#state{hashtrees=undefined}
      end;
    _ ->
      State#state{upgrade_hashtree=false}
  end.
do_put(Sender, {Bucket,_Key}=BKey, RObj, ReqID, StartTime, Options, State) ->
  BProps =  case proplists:get_value(bucket_props, Options) of
              undefined ->
                riak_core_bucket:get_bucket(Bucket);
              Props ->
                Props
            end,
  ReadRepair = proplists:get_value(rr, Options, false),
  PruneTime = case ReadRepair of
                true ->
                  undefined;
                false ->
                  StartTime
              end,
  Coord = proplists:get_value(coord, Options, false),
  CRDTOp = proplists:get_value(counter_op, Options, proplists:get_value(crdt_op, Options, undefined)),
  PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false) orelse Coord,
    coord=Coord,
    lww=proplists:get_value(last_write_wins, BProps, false),
    bkey=BKey,
    robj=RObj,
    reqid=ReqID,
    bprops=BProps,
    starttime=StartTime,
    readrepair = ReadRepair,
    prunetime=PruneTime,
    crdt_op = CRDTOp},
  {PrepPutRes, UpdPutArgs, State2} = prepare_put(State, PutArgs),
  {Reply, UpdState} = perform_put(PrepPutRes, State2, UpdPutArgs),
  riak_core_vnode:reply(Sender, Reply),

  update_index_write_stats(UpdPutArgs#putargs.is_index, UpdPutArgs#putargs.index_specs),
  {Reply, UpdState}.
clear_vnodeid(StatusMgr) ->
    riak_kv_vnode_status_mgr:clear_vnodeid(StatusMgr).
fold_fun(buckets, BufferMod, none, _Extra) ->
  fun(Bucket, Buffer) ->
    BufferMod:add(Bucket, Buffer)
  end;
fold_fun(buckets, BufferMod, Filter, _Extra) ->
  fun(Bucket, Buffer) ->
    case Filter(Bucket) of
      true ->
        BufferMod:add(Bucket, Buffer);
      false ->
        Buffer
    end
  end;
fold_fun(keys, BufferMod, none, undefined) ->
  fun(_, Key, Buffer) ->
    BufferMod:add(Key, Buffer)
  end;
fold_fun(keys, BufferMod, none, {Bucket, Index, N, NumPartitions}) ->
  fun(_, Key, Buffer) ->
    Hash = riak_core_util:chash_key({Bucket, Key}),
    case riak_core_ring:future_index(Hash, Index, N, NumPartitions, NumPartitions) of
      Index ->
        BufferMod:add(Key, Buffer);
      _ ->
        Buffer
    end
  end;
fold_fun(keys, BufferMod, Filter, undefined) ->
  fun(_, Key, Buffer) ->
    case Filter(Key) of
      true ->
        BufferMod:add(Key, Buffer);
      false ->
        Buffer
    end
  end;
fold_fun(keys, BufferMod, Filter, {Bucket, Index, N, NumPartitions}) ->
  fun(_, Key, Buffer) ->
    Hash = riak_core_util:chash_key({Bucket, Key}),
    case riak_core_ring:future_index(Hash, Index, N, NumPartitions, NumPartitions) of
      Index ->
        case Filter(Key) of
          true ->
            BufferMod:add(Key, Buffer);
          false ->
            Buffer
        end;
      _ ->
        Buffer
    end
  end.

encode_binary_object(Bucket, Key, Value) ->
    Method = handoff_data_encoding_method(),

    case Method of
        encode_raw  -> EncodedObject = { Bucket, Key, iolist_to_binary(Value) },
                       return_encoded_binary_object(Method, EncodedObject);

        %% zlib encoding is a special case, we return the legacy format:
        encode_zlib -> PBEncodedObject = riak_core_pb:encode_riakobject_pb(#riakobject_pb{bucket=Bucket, key=Key, val=Value}),
                       zlib:zip(PBEncodedObject)
    end.

%% Return objects in a consistent form:
return_encoded_binary_object(Method, EncodedObject) ->
    term_to_binary({ Method, EncodedObject }).
%% @private
result_fun(Sender) ->
    fun(Items) ->
            riak_core_vnode:reply(Sender, Items)
    end.

%% @private
result_fun(Bucket, Sender) ->
    fun(Items) ->
            riak_core_vnode:reply(Sender, {Bucket, Items})
    end.
finish_fun(BufferMod, Sender) ->
    fun(Buffer) ->
            finish_fold(BufferMod, Buffer, Sender)
    end.
result_fun_ack(Bucket, Sender) ->
    fun(Items) ->
            Monitor = riak_core_vnode:monitor(Sender),
            riak_core_vnode:reply(Sender, {{self(), Monitor}, Bucket, Items}),
            receive
                {Monitor, ok} ->
                    erlang:demonitor(Monitor, [flush]);
                {Monitor, stop_fold} ->
                    erlang:demonitor(Monitor, [flush]),
                    throw(stop_fold);
                {'DOWN', Monitor, process, _Pid, _Reason} ->
                    throw(receiver_down)
            end
    end.
fold_extras_keys(Index, Bucket) ->
  case app_helper:get_env(riak_kv, fold_preflist_filter, false) of
    true ->
      {ok, R} = riak_core_ring_manager:get_my_ring(),
      NValMap = nval_map(R),
      N = case lists:keyfind(Bucket, 1, NValMap) of
            false -> riak_core_bucket:default_object_nval();
            {Bucket, NVal} -> NVal
          end,
      NumPartitions = riak_core_ring:num_partitions(R),
      {Bucket, Index, N, NumPartitions};
    false ->
      undefined
  end.
finish_fold(BufferMod, Buffer, Sender) ->
    BufferMod:flush(Buffer),
    riak_core_vnode:reply(Sender, done).
handle_coverage_index(Bucket, ItemFilter, Query,
                      FilterVNodes, Sender,
                      State=#state{mod=Mod,
                                   key_buf_size=DefaultBufSz,
                                   modstate=ModState},
                      ResultFunFun) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case IndexBackend of
        true ->
            %% Update stats...
            ok = riak_kv_stat:update(vnode_index_read),

            ResultFun = ResultFunFun(Bucket, Sender),
            BufSize = buffer_size_for_index_query(Query, DefaultBufSz),
            Opts = [{index, Bucket, prepare_index_query(Query)},
                    {bucket, Bucket}, {buffer_size, BufSize}],
            %% @HACK
            %% Really this should be decided in the backend
            %% if there was a index_query fun.
            FoldType = case riak_index:return_body(Query) of
                           true -> fold_objects;
                           false -> fold_keys
                       end,
            handle_coverage_fold(FoldType, Bucket, ItemFilter, ResultFun,
                                    FilterVNodes, Sender, Opts, State);
        false ->
            {reply, {error, {indexes_not_supported, Mod}}, State}
    end.
list(FoldFun, FinishFun, Mod, ModFun, ModState, Opts, Buffer) ->
    case Mod:ModFun(FoldFun, Buffer, Opts, ModState) of
        {ok, Acc} ->
            FinishFun(Acc);
        {async, AsyncWork} ->
            {async, AsyncWork}
    end.
decode_binary_object(BinaryObject) ->
    try binary_to_term(BinaryObject) of
        { Method, BinObj } ->
                                case Method of
                                    encode_raw  -> {B, K, Val} = BinObj,
                                                   BKey = {B, K},
                                                   {BKey, Val};

                                    _           -> lager:error("Invalid handoff encoding ~p", [Method]),
                                                   throw(invalid_handoff_encoding)
                                end;

        _                   ->  lager:error("Request to decode invalid handoff object"),
                                throw(invalid_handoff_object)

    %% An exception means we have a legacy handoff object:
    catch
        _:_                 -> do_zlib_decode(BinaryObject)
    end.
do_diffobj_put({Bucket, Key}=BKey, DiffObj,
    StateData=#state{mod=Mod,
      modstate=ModState,
      idx=Idx}) ->
  StartTS = os:timestamp(),
  {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
  IndexBackend = lists:member(indexes, Capabilities),
  maybe_cache_evict(BKey, StateData),
  case do_get_object(Bucket, Key, Mod, ModState) of
    {error, not_found, _UpdModState} ->
      case IndexBackend of
        true ->
          IndexSpecs = riak_object:index_specs(DiffObj);
        false ->
          IndexSpecs = []
      end,
      case encode_and_put(DiffObj, Mod, Bucket, Key,
        IndexSpecs, ModState, no_max_check) of
        {{ok, _UpdModState} = InnerRes, _EncodedVal} ->
          update_hashtree(Bucket, Key, DiffObj, StateData),
          update_index_write_stats(IndexBackend, IndexSpecs),
          update_vnode_stats(vnode_put, Idx, StartTS),
          ?INDEX({DiffObj, no_old_object}, handoff, Idx),
          InnerRes;
        {InnerRes, _Val} ->
          InnerRes
      end;
    {ok, OldObj, _UpdModState} ->
      %% Merge handoff values with the current - possibly discarding
      %% if out of date.  Ok to set VId/Starttime undefined as
      %% they are not used for non-coordinating puts.
      case put_merge(false, false, OldObj, DiffObj, undefined, undefined) of
        {oldobj, _} ->
          {ok, ModState};
        {newobj, NewObj} ->
          AMObj = enforce_allow_mult(NewObj, riak_core_bucket:get_bucket(Bucket)),
          case IndexBackend of
            true ->
              IndexSpecs = riak_object:diff_index_specs(AMObj, OldObj);
            false ->
              IndexSpecs = []
          end,
          case encode_and_put(AMObj, Mod, Bucket, Key,
            IndexSpecs, ModState, no_max_check) of
            {{ok, _UpdModState} = InnerRes, _EncodedVal} ->
              update_hashtree(Bucket, Key, AMObj, StateData),
              update_index_write_stats(IndexBackend, IndexSpecs),
              update_vnode_stats(vnode_put, Idx, StartTS),
              ?INDEX({AMObj, OldObj}, handoff, Idx),
              InnerRes;
            {InnerRes, _EncodedVal} ->
              InnerRes
          end
      end
  end.
maybe_get_vnode_lock(SrcPartition, Pid) ->
    case riak_core_bg_manager:use_bg_mgr(riak_kv, handoff_use_background_manager) of
        true  ->
            Lock = ?KV_VNODE_LOCK(SrcPartition),
            case riak_core_bg_manager:get_lock(Lock, Pid, [{task, handoff}]) of
                {ok, _Ref} -> ok;
                max_concurrency -> max_concurrency
            end;
        false ->
            ok
    end.
maybe_calc_handoff_size(#state{mod=Mod,modstate=ModState}) ->
  {ok, Capabilities} = Mod:capabilities(ModState),
  case lists:member(size, Capabilities) of
    true -> Mod:data_size(ModState);
    false -> undefined
  end.
update_index_write_stats(false, _IndexSpecs) ->
    ok;
update_index_write_stats(true, IndexSpecs) ->
    {Added, Removed} = count_index_specs(IndexSpecs),
    ok = riak_kv_stat:update({vnode_index_write, Added, Removed}).
update_hashtree(Bucket, Key, RObj, #state{hashtrees=Trees}) ->
  Items = [{object, {Bucket, Key}, RObj}],
  case get_hashtree_token() of
    true ->
      riak_kv_index_hashtree:async_insert(Items, [], Trees),
      ok;
    false ->
      riak_kv_index_hashtree:insert(Items, [], Trees),
      put(hashtree_tokens, max_hashtree_tokens()),
      ok
  end.
max_hashtree_tokens() ->
    app_helper:get_env(riak_kv,
                       anti_entropy_max_async,
                       ?DEFAULT_HASHTREE_TOKENS).
get_hashtree_token() ->
    Tokens = get(hashtree_tokens),
    case Tokens of
        undefined ->
            put(hashtree_tokens, max_hashtree_tokens() - 1),
            true;
        N when N > 0 ->
            put(hashtree_tokens, Tokens - 1),
            true;
        _ ->
            false
    end.

count_index_specs(IndexSpecs) ->
    %% Count index specs...
    F = fun({add, _, _}, {AddAcc, RemoveAcc}) ->
                {AddAcc + 1, RemoveAcc};
           ({remove, _, _}, {AddAcc, RemoveAcc}) ->
                {AddAcc, RemoveAcc + 1}
        end,
    lists:foldl(F, {0, 0}, IndexSpecs).

encode_and_put(Obj, Mod, Bucket, Key, IndexSpecs, ModState, MaxCheckFlag) ->
    DoMaxCheck = MaxCheckFlag == do_max_check,
    NumSiblings = riak_object:value_count(Obj),
    case DoMaxCheck andalso
         NumSiblings > app_helper:get_env(riak_kv, max_siblings) of
        true ->
            lager:error("Put failure: too many siblings for object ~p/~p (~p)",
                        [Bucket, Key, NumSiblings]),
            {{error, {too_many_siblings, NumSiblings}, ModState},
             undefined};
        false ->
            case NumSiblings > app_helper:get_env(riak_kv, warn_siblings) of
                true ->
                    lager:warning("Too many siblings for object ~p/~p (~p)",
                                  [Bucket, Key, NumSiblings]);
                false ->
                    ok
            end,
            encode_and_put_no_sib_check(Obj, Mod, Bucket, Key, IndexSpecs,
                                        ModState, MaxCheckFlag)
    end.

encode_and_put_no_sib_check(Obj, Mod, Bucket, Key, IndexSpecs, ModState,
                            MaxCheckFlag) ->
    DoMaxCheck = MaxCheckFlag == do_max_check,
    case uses_r_object(Mod, ModState, Bucket) of
        true ->
            %% Non binary returning backends will have to handle size warnings
            %% and errors themselves.
            Mod:put_object(Bucket, Key, IndexSpecs, Obj, ModState);
        false ->
            ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
            EncodedVal = riak_object:to_binary(ObjFmt, Obj),
            BinSize = size(EncodedVal),
            %% Report or fail on large objects
            case DoMaxCheck andalso
                 BinSize > app_helper:get_env(riak_kv, max_object_size) of
                true ->
                    lager:error("Put failure: object too large to write ~p/~p ~p bytes",
                                [Bucket, Key, BinSize]),
                    {{error, {too_large, BinSize}, ModState},
                     EncodedVal};
                false ->
                    WarnSize = app_helper:get_env(riak_kv, warn_object_size),
                    case BinSize > WarnSize of
                       true ->
                            lager:warning("Writing very large object " ++
                                          "(~p bytes) to ~p/~p",
                                          [BinSize, Bucket, Key]);
                        false ->
                            ok
                    end,
                    PutRet = Mod:put(Bucket, Key, IndexSpecs, EncodedVal,
                                     ModState),
                    {PutRet, EncodedVal}
            end
    end.

prepare_put(State=#state{vnodeid=VId,
  mod=Mod,
  modstate=ModState},
    PutArgs=#putargs{bkey={Bucket, _Key},
      lww=LWW,
      coord=Coord,
      robj=RObj,
      starttime=StartTime,
      bprops = BProps}) ->
  %% Can we avoid reading the existing object? If this is not an
  %% index backend, and the bucket is set to last-write-wins, then
  %% no need to incur additional get. Otherwise, we need to read the
  %% old object to know how the indexes have changed.
  IndexBackend = is_indexed_backend(Mod, Bucket, ModState),
  IsSearchable = ?IS_SEARCH_ENABLED_FOR_BUCKET(BProps),
  SkipReadBeforeWrite = LWW andalso (not IndexBackend) andalso (not IsSearchable),
  case SkipReadBeforeWrite of
    true ->
      prepare_blind_put(Coord, RObj, VId, StartTime, PutArgs, State);
    false ->
      prepare_read_before_write_put(State, PutArgs, IndexBackend, IsSearchable)
  end.
perform_put({fail, _, _}=Reply, State, _PutArgs) ->
    {Reply, State};
perform_put({false, {Obj, _OldObj}},
            #state{idx=Idx}=State,
            #putargs{returnbody=true,
                     reqid=ReqID}) ->
    {{dw, Idx, Obj, ReqID}, State};
perform_put({false, {_Obj, _OldObj}},
            #state{idx=Idx}=State,
            #putargs{returnbody=false,
                     reqid=ReqId}) ->
    {{dw, Idx, ReqId}, State};
perform_put({true, {_Obj, _OldObj}=Objects},
            State,
            #putargs{returnbody=RB,
                     bkey=BKey,
                     reqid=ReqID,
                     index_specs=IndexSpecs,
                     readrepair=ReadRepair}) ->
    case ReadRepair of
      true ->
        MaxCheckFlag = no_max_check;
      false ->
        MaxCheckFlag = do_max_check
    end,
    {Reply, State2} = actual_put(BKey, Objects, IndexSpecs, RB, ReqID, MaxCheckFlag, State),
    {Reply, State2}.
handoff_data_encoding_method() ->
    riak_core_capability:get({riak_kv, handoff_data_encoding}, encode_zlib).
%%actual_put(BKey, {Obj, OldObj}, IndexSpecs, RB, ReqID, State) ->
%%    actual_put(BKey, {Obj, OldObj}, IndexSpecs, RB, ReqID, do_max_check, State).

actual_put(BKey={Bucket, Key}, {Obj, OldObj}, IndexSpecs, RB, ReqID, MaxCheckFlag,
           State=#state{idx=Idx,
                        mod=Mod,
                        modstate=ModState}) ->
    case encode_and_put(Obj, Mod, Bucket, Key, IndexSpecs, ModState,
                       MaxCheckFlag) of
        {{ok, UpdModState}, EncodedVal} ->
            update_hashtree(Bucket, Key, EncodedVal, State),
            maybe_cache_object(BKey, Obj, State),
            ?INDEX({Obj, OldObj}, put, Idx),
            Reply = case RB of
                true ->
                    {dw, Idx, Obj, ReqID};
                false ->
                    {dw, Idx, ReqID}
            end;
        {{error, Reason, UpdModState}, _EncodedVal} ->
            Reply = {fail, Idx, Reason}
    end,
    {Reply, State#state{modstate=UpdModState}}.
nval_map(Ring) ->
    riak_core_bucket:bucket_nval_map(Ring).
buffer_size_for_index_query(#riak_kv_index_v3{max_results=N}, DefaultSize)
  when is_integer(N), N < DefaultSize ->
    N;
buffer_size_for_index_query(_Q, DefaultSize) ->
    DefaultSize.
prepare_index_query(#riak_kv_index_v3{term_regex=RE} = Q) when
        RE =/= undefined ->
    {ok, CompiledRE} = re:compile(RE),
    Q#riak_kv_index_v3{term_regex=CompiledRE};
prepare_index_query(Q) ->
    Q.
do_zlib_decode(BinaryObject) ->
    DecodedObject = zlib:unzip(BinaryObject),
    PBObj = riak_core_pb:decode_riakobject_pb(DecodedObject),
    BKey = {PBObj#riakobject_pb.bucket,PBObj#riakobject_pb.key},
    {BKey, PBObj#riakobject_pb.val}.
maybe_cache_evict(BKey, #state{md_cache = MDCache}) ->
    case MDCache of
        undefined ->
            ok;
        _ ->
            ets:delete(MDCache, BKey)
    end.
put_merge(false, true, _CurObj, UpdObj, _VId, _StartTime) -> % coord=false, LWW=true
    %% @TODO Do we need to mark the clock dirty here? I think so
    %% @TODO Check the clock of the incoming object, if it is more advanced
    %% for our actor than we are then something is amiss, and we need
    %% to mark the actor as dirty for this key
    {newobj, UpdObj};
put_merge(false, false, CurObj, UpdObj, _VId, _StartTime) -> % coord=false, LWW=false
    %% a downstream merge, or replication of a coordinated PUT
    %% Merge the value received with local replica value
    %% and store the value IFF it is different to what we already have
    %%
    %% @TODO Check the clock of the incoming object, if it is more advanced
    %% for our actor than we are then something is amiss, and we need
    %% to mark the actor as dirty for this key
    ResObj = riak_object:syntactic_merge(CurObj, UpdObj),
    case riak_object:equal(ResObj, CurObj) of
        true ->
            {oldobj, CurObj};
        false ->
            {newobj, ResObj}
    end;
put_merge(true, LWW, CurObj, UpdObj, VId, StartTime) ->
    %% @TODO If the current object has a dirty clock, we need to start
    %% a new per key epoch and mark clock as clean.
    {newobj, riak_object:update(LWW, CurObj, UpdObj, VId, StartTime)}.
enforce_allow_mult(Obj, BProps) ->
    case proplists:get_value(allow_mult, BProps) of
        true -> Obj;
        _ ->
            case riak_object:get_contents(Obj) of
                [_] -> Obj;
                Mult ->
                    {MD, V} = select_newest_content(Mult),
                    riak_object:set_contents(Obj, [{MD, V}])
            end
    end.
select_newest_content(Mult) ->
  hd(lists:sort(
    fun({MD0, _}, {MD1, _}) ->
      riak_core_util:compare_dates(
        dict:fetch(<<"X-Riak-Last-Modified">>, MD0),
        dict:fetch(<<"X-Riak-Last-Modified">>, MD1))
    end,
    Mult)).
is_indexed_backend(Mod, Bucket, ModState) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    IndexBackend.
prepare_blind_put(Coord, RObj, VId, StartTime, PutArgs, State) ->
    ObjToStore = case Coord of
        true ->
            %% Do we need to use epochs here? I guess we
            %% don't care, and since we don't read, we
            %% can't.
            riak_object:increment_vclock(RObj, VId, StartTime);
        false ->
            RObj
    end,
    {{true, {ObjToStore, no_old_object}}, PutArgs#putargs{is_index = false}, State}.
prepare_read_before_write_put(#state{mod = Mod,
  modstate = ModState,
  md_cache = MDCache}=State,
    #putargs{bkey={Bucket, Key}=BKey,
      robj=RObj}=PutArgs,
    IndexBackend, IsSearchable) ->
  {CacheClock, CacheData} = maybe_check_md_cache(MDCache, BKey),

  RequiresGet = determine_requires_get(CacheClock, RObj, IsSearchable),
  GetReply = get_old_object_or_fake(RequiresGet, Bucket, Key, Mod, ModState, CacheClock),
  case GetReply of
    not_found ->
      prepare_put_new_object(State, PutArgs, IndexBackend);
    {ok, OldObj} ->
      prepare_put_existing_object(State, PutArgs, OldObj, IndexBackend, CacheData, RequiresGet)
  end.
maybe_check_md_cache(Table, BKey) ->
    case Table of
        undefined ->
            {undefined, undefined};
        _ ->
            case ets:lookup(Table, BKey) of
                [{_TS, BKey, MD}] ->
                    MD;
                [] ->
                    {undefined, undefined}
            end
    end.
determine_requires_get(CacheClock, RObj, IsSearchable) ->
  RequiresGet =
    case CacheClock of
      undefined ->
        true;
      Clock ->
        %% We need to perform a local get, to merge contents,
        %% if the local object has events unseen by the
        %% incoming object. If the incoming object descends
        %% the cache (i.e. has seen all its events) no need to
        %% do a local get and merge, just overwrite.
        not riak_object:vclock_descends(RObj, Clock) orelse IsSearchable
    end,
  RequiresGet.
get_old_object_or_fake(false, Bucket, Key, _Mod, _ModState, CacheClock) ->
    FakeObj0 = riak_object:new(Bucket, Key, <<>>),
    FakeObj = riak_object:set_vclock(FakeObj0, CacheClock),
    {ok, FakeObj}.
prepare_put_new_object(#state{idx =Idx} = State,
               #putargs{robj = RObj,
                        coord=Coord,
                        starttime=StartTime,
                        crdt_op=CRDTOp} = PutArgs,
                       IndexBackend) ->
    IndexSpecs = case IndexBackend of
                     true ->
                         riak_object:index_specs(RObj);
                     false ->
                         []
                 end,
    {EpochId, State2} = new_key_epoch(State),
    RObj2 = maybe_update_vclock(Coord, RObj, EpochId, StartTime),
    RObj3 = maybe_do_crdt_update(Coord, CRDTOp, EpochId, RObj2),
    determine_put_result(RObj3, no_old_object, Idx, PutArgs, State2, IndexSpecs, IndexBackend).

new_key_epoch(State=#state{vnodeid=VId, counter=#counter_state{use=false}}) ->
    {VId, State};
new_key_epoch(State) ->
    NewState=#state{counter=#counter_state{cnt=Cntr}, vnodeid=VId} = update_counter(State),
    EpochId = key_epoch_actor(VId, Cntr),
    {EpochId, NewState}.

maybe_do_crdt_update(_Coord = _, undefined, _VId, RObj) ->
    RObj;
maybe_do_crdt_update(_Coord = true, CRDTOp, VId, RObj) ->
    do_crdt_update(RObj, VId, CRDTOp);
maybe_do_crdt_update(_Coord = false, _CRDTOp, _Vid, RObj) ->
    RObj.

do_crdt_update(RObj, VId, CRDTOp) ->
    {Time, Value} = timer:tc(riak_kv_crdt, update, [RObj, VId, CRDTOp]),
    ok = riak_kv_stat:update({vnode_dt_update, get_crdt_mod(CRDTOp), Time}),
    Value.


determine_put_result({error, E}, _, Idx, PutArgs, State, _IndexSpecs, _IndexBackend) ->
    {{fail, Idx, E}, PutArgs, State};
determine_put_result(ObjToStore, OldObj, _Idx, PutArgs, State, IndexSpecs, IndexBackend) ->
    {{true, {ObjToStore, OldObj}},
     PutArgs#putargs{index_specs = IndexSpecs,
                     is_index    = IndexBackend}, State}.


prepare_put_existing_object(#state{idx =Idx} = State,
                    #putargs{coord=Coord,
                             robj = RObj,
                             lww=LWW,
                             starttime = StartTime,
                             bprops = BProps,
                             prunetime=PruneTime,
                             crdt_op = CRDTOp}=PutArgs,
                            OldObj, IndexBackend, CacheData, RequiresGet) ->
    {ActorId, State2} = maybe_new_key_epoch(Coord, State, OldObj, RObj),
    case put_merge(Coord, LWW, OldObj, RObj, ActorId, StartTime) of
        {oldobj, OldObj} ->
            {{false, {OldObj, no_old_object}}, PutArgs, State2};
        {newobj, NewObj} ->
            AMObj = enforce_allow_mult(NewObj, BProps),
            IndexSpecs = get_index_specs(IndexBackend, CacheData, RequiresGet, AMObj, OldObj),
            ObjToStore0 = maybe_prune_vclock(PruneTime, AMObj, BProps),
            ObjectToStore = maybe_do_crdt_update(Coord, CRDTOp, ActorId, ObjToStore0),
            determine_put_result(ObjectToStore, OldObj, Idx, PutArgs, State2, IndexSpecs, IndexBackend)
    end.
maybe_update_vclock(_Coord=true, RObj, VId, StartTime) ->
    riak_object:increment_vclock(RObj, VId, StartTime);
maybe_update_vclock(_Coord=false, RObj, _VId, _StartTime) ->
    %% @TODO Not coordindating, not found local, is there an entry for
    %% us in the clock? If so, mark as dirty
    RObj.
update_counter(State=#state{counter=CounterState}) ->
    #counter_state{cnt=Counter0} = CounterState,
    Counter = Counter0 +  1,
    maybe_lease_counter(State#state{counter=CounterState#counter_state{cnt=Counter}}).
maybe_lease_counter(#state{vnodeid=VId, counter=#counter_state{cnt=Cnt, lease=Lease}})
  when Cnt > Lease ->
  %% Holy broken invariant. Log and crash.
  lager:error("Broken invariant, epoch counter ~p greater than lease ~p for vnode ~p. Crashing.",
    [Cnt, Lease, VId]),
  exit(epoch_counter_invariant_broken);
maybe_lease_counter(State=#state{counter=#counter_state{cnt=Lease, lease=Lease,
  leasing=true}}) ->
  %% Block until we get a new lease, or crash the vnode
  {ok, NewState} = blocking_lease_counter(State),
  NewState;
maybe_lease_counter(State=#state{counter=#counter_state{leasing=true}}) ->
  %% not yet at the blocking stage, waiting on a lease
  State;
maybe_lease_counter(State) ->
  #state{status_mgr_pid=MgrPid, counter=CS=#counter_state{cnt=Cnt, lease=Lease,
    lease_size=LeaseSize}} = State,
  %% @TODO (rdb) configurable??
  %% has more than 80% of the lease been used?
  CS2 = if (Lease - Cnt) =< 0.2 * LeaseSize  ->
    ok = riak_kv_vnode_status_mgr:lease_counter(MgrPid, LeaseSize),
    CS#counter_state{leasing=true};
          ?ELSE ->
            CS
        end,
  State#state{counter=CS2}.
key_epoch_actor(ActorBin, Cntr) ->
    <<ActorBin/binary, Cntr:32/integer>>.

get_crdt_mod(Int) when is_integer(Int) -> ?COUNTER_TYPE;
get_crdt_mod(#crdt_op{mod=Mod}) -> Mod;
get_crdt_mod(Atom) when is_atom(Atom) -> Atom.
maybe_new_key_epoch(false, State, _, _) ->
  %% Never add a new key epoch when not coordinating
  %% @TODO (rdb) need to mark actor as dirty though.
  {State#state.vnodeid, State};
maybe_new_key_epoch(true, State=#state{counter=#counter_state{use=false}, vnodeid=VId}, _, _) ->
  %% Per-Key-Epochs is off, use the base vnodeid
  {VId, State};
maybe_new_key_epoch(true, State, LocalObj, IncomingObj) ->
  #state{vnodeid=VId} = State,
  %% @TODO (rdb) maybe optimise since highly likey both objects
  %% share the majority of actors, maybe a single umerged list of
  %% actors, somehow tagged by local | incoming?
  case highest_actor(VId, LocalObj) of
    {undefined, 0, 0} -> %% Not present locally
      %% Never acted on this object before, new epoch.
      new_key_epoch(State);
    {LocalId, LocalEpoch, LocalCntr} -> %% Present locally
      case highest_actor(VId, IncomingObj) of
        {_InId, InEpoch, InCntr} when InEpoch > LocalEpoch;
          InCntr > LocalCntr ->
          %% In coming actor-epoch or counter greater than
          %% local, some byzantine failure, new epoch.
          B = riak_object:bucket(LocalObj),
          K = riak_object:key(LocalObj),

          lager:warning("Inbound clock entry for ~p in ~p/~p greater than local." ++
          "Epochs: {In:~p Local:~p}. Counters: {In:~p Local:~p}.",
            [VId, B, K, InEpoch, LocalEpoch, InCntr, LocalCntr]),
          new_key_epoch(State);
        _ ->
          %% just use local id
          %% Return the highest local epoch ID for this
          %% key. This may be the pre-epoch ID (i.e. no
          %% epoch), which is good, no reason to force a new
          %% epoch on all old keys.
          {LocalId, State}
      end
  end.



get_index_specs(_IndexedBackend=true, CacheData, RequiresGet, NewObj, OldObj) ->
    case CacheData /= undefined andalso
         RequiresGet == false of
        true ->
            NewData = riak_object:index_data(NewObj),
            riak_object:diff_index_data(NewData,
                                        CacheData);
        false ->
            riak_object:diff_index_specs(NewObj,
                                         OldObj)
    end;

get_index_specs(_IndexedBackend=false, _CacheData, _RequiresGet, _NewObj, _OldObj) ->
    [].

maybe_prune_vclock(_PruneTime=undefined, RObj, _BProps) ->
    RObj;
maybe_prune_vclock(PruneTime, RObj, BProps) ->
    riak_object:prune_vclock(RObj, PruneTime, BProps).

blocking_lease_counter(State) ->
    {MaxErrs, MaxTime} = get_counter_wait_values(),
    blocking_lease_counter(State, {0, MaxErrs, MaxTime}).

blocking_lease_counter(_State, {MaxErrs, MaxErrs, _MaxTime}) ->
    {error, counter_lease_max_errors};
blocking_lease_counter(State, {ErrCnt, MaxErrors, MaxTime}) ->
    #state{idx=Index, vnodeid=VId, status_mgr_pid=Pid, counter=CounterState} = State,
    #counter_state{lease_size=LeaseSize, use=UseEpochCounter} = CounterState,
    Start = os:timestamp(),
    receive
        {'EXIT', Pid, Reason} ->
            lager:error("Failed to lease counter for ~p : ~p", [Index, Reason]),
            {ok, NewPid} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),
            ok = riak_kv_vnode_status_mgr:lease_counter(NewPid, LeaseSize),
            NewState = State#state{status_mgr_pid=NewPid},
            Elapsed = timer:now_diff(os:timestamp(), Start),
            blocking_lease_counter(NewState, {ErrCnt+1, MaxErrors, MaxTime - Elapsed});
        {counter_lease, {Pid, VId, NewLease}} ->
            NewCS = CounterState#counter_state{lease=NewLease, leasing=false},
            {ok, State#state{counter=NewCS}};
        {counter_lease, {Pid, NewVId, NewLease}} ->
            lager:info("New Vnode id for ~p. Epoch counter rolled over.", [Index]),
            NewCS = CounterState#counter_state{lease=NewLease, leasing=false, cnt=1},
            {ok, State#state{vnodeid=NewVId, counter=NewCS}}
    after
        MaxTime ->
            {error, counter_lease_timeout}
    end.

highest_actor(ActorBase, Obj) ->
    ActorSize = size(ActorBase),
    Actors = riak_object:all_actors(Obj),

    {Actor, Epoch} = lists:foldl(fun(Actor, {HighestActor, HighestEpoch}) ->
                                         case Actor of
                                             <<ActorBase:ActorSize/binary, Epoch:32/integer>>
                                               when Epoch > HighestEpoch ->
                                                 {Actor, Epoch};
                                             %% Since an actor without
                                             %% an epoch is lower than
                                             %% an actor with one,
                                             %% this means in the
                                             %% unmatched case, `undefined'
                                             %% through as the highest
                                             %% actor, and the epoch
                                             %% (of zero) passes
                                             %% through too.
                                             _ ->  {HighestActor, HighestEpoch}
                                         end
                                 end,
                                 {undefined, 0},
                                 Actors),
    %% get the greatest event for the highest/latest actor
    {Actor, Epoch, riak_object:actor_counter(Actor, Obj)}.
get_counter_wait_values() ->
    MaxErrors = non_neg_env(riak_kv, counter_lease_errors, ?DEFAULT_CNTR_LEASE_ERRS),
    MaxTime = non_neg_env(riak_kv, counter_lease_timeout, ?DEFAULT_CNTR_LEASE_TO),
    {MaxErrors, MaxTime}.


handle_info({ensemble_get, Key, From}, State=#state{idx=Idx, forward=Fwd}) ->
    case Fwd of
        undefined ->
            {reply, {r, Retval, _, _}, State2} = do_get(undefined, Key, undefined, State),
            Reply = case Retval of
                        {ok, Obj} ->
                            Obj;
                        _ ->
                            notfound
                    end,
            riak_kv_ensemble_backend:reply(From, Reply),
            {ok, State2};
        Fwd when is_atom(Fwd) ->
            forward_get({Idx, Fwd}, Key, From),
            {ok, State}
    end;


handle_info({raw_forward_get, Key, From}, State) ->
  {reply, {r, Retval, _, _}, State2} = do_get(undefined, Key, undefined, State),
  Reply = case Retval of
            {ok, Obj} ->
              Obj;
            _ ->
              notfound
          end,
  riak_kv_ensemble_backend:reply(From, Reply),
  {ok, State2}.
forward_get({Idx, Node}, Key, From) ->
    Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    riak_core_send_msg:bang_unreliable(Proxy, {raw_forward_get, Key, From}),
    ok.