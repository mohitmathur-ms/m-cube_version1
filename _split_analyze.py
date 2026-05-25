import ast

src = open('core/backtest_runner.py', encoding='utf-8').read()
tree = ast.parse(src)

MAP = {
 '_phase':'profiling','_config_supports_extra_bar_types':'profiling','_config_supports_aggregate_to':'profiling',
 '_aggregate_target_for_slot':'bar_types','_group_slots':'bar_types','_pair_bid_ask_bar_type':'bar_types',
 '_DAY_NAME_TO_WEEKDAY':'bar_filters','_allowed_weekdays':'bar_filters','_NANOS_PER_DAY':'bar_filters','_EPOCH_WEEKDAY':'bar_filters',
 '_filter_bars_by_weekday':'bar_filters','_filter_bars_after_ns':'bar_filters','_NANOS_PER_MINUTE':'bar_filters',
 '_hhmm_to_minute':'bar_filters','_is_intraday_bar_type':'bar_filters','_filter_bars_by_time_of_day':'bar_filters',
 '_path_b_active':'path_b','_sec_to_hms':'path_b','_chunk_data_configs_for_path_b':'path_b','_build_run_config':'path_b','_path_b_supports_filters':'path_b',
 '_RBOSettings':'rbo','_hms_to_sec':'rbo','add_one_hour':'rbo','_apply_winter_time':'rbo','_resolve_rbo':'rbo',
 '_OtherSettings':'other_settings','_VALID_ON_SL_ACTION_ON':'other_settings','_VALID_ON_TARGET_ACTION_ON':'other_settings','_resolve_other_settings':'other_settings',
 '_PfStoplossSettings':'portfolio_exit_config','_PfTargetSettings':'portfolio_exit_config','_MoveSLConfig':'portfolio_exit_config',
 '_VALID_PF_SL_TYPES_FX':'portfolio_exit_config','_UNDERLYING_PF_SL_TYPES':'portfolio_exit_config','_VALID_PF_TGT_TYPES_FX':'portfolio_exit_config',
 '_UNDERLYING_PF_TGT_TYPES':'portfolio_exit_config','_OPTIONS_ONLY_PF_SL_TYPES':'portfolio_exit_config','_OPTIONS_ONLY_PF_TGT_TYPES':'portfolio_exit_config',
 '_VALID_MOVE_SL_ACTIONS_FX':'portfolio_exit_config','_OPTIONS_ONLY_MOVE_SL_ACTIONS':'portfolio_exit_config',
 '_resolve_pf_stoploss':'portfolio_exit_config','_resolve_pf_target':'portfolio_exit_config','_resolve_move_sl_to_cost':'portfolio_exit_config',
 '_VALID_PF_ACTIONS_FX':'cross_portfolio','_OPTIONS_ONLY_PF_ACTIONS':'cross_portfolio','_CROSS_PORTFOLIO_ACTIONS':'cross_portfolio',
 '_CROSS_PORTFOLIO_EVENT_BUS':'cross_portfolio','publish_cross_portfolio_event':'cross_portfolio','consume_cross_portfolio_events':'cross_portfolio',
 'clear_cross_portfolio_bus':'cross_portfolio','_LEGACY_PF_ACTION_ALIASES':'cross_portfolio','_normalize_pf_action':'cross_portfolio',
 '_REEXECUTE_FAMILY_ACTIONS':'cross_portfolio','_ENTRY_PRICE_REEXEC_ACTIONS':'cross_portfolio','_is_reexec_action':'cross_portfolio','_is_entry_price_reexec':'cross_portfolio',
 '_entry_at_clip':'portfolio_clip','_AggCoordination':'portfolio_clip','_compute_agg_coordination':'portfolio_clip','_ClipResult':'portfolio_clip',
 '_ts_iso_to_ns':'portfolio_clip','_apply_portfolio_clip':'portfolio_clip','_slot_pnl_at_ts':'portfolio_clip','_build_clip_result':'portfolio_clip',
 '_build_underlying_curve':'portfolio_clip','_underlying_sl_clip':'portfolio_clip','_user_sl_clip':'portfolio_clip','_underlying_tgt_clip':'portfolio_clip',
 '_user_tgt_clip':'portfolio_clip','_earliest_clip':'portfolio_clip',
 '_merge_equity_curves':'equity_curves','_build_equity_curve_from_account':'equity_curves','_ensure_final_equity_point':'equity_curves',
 '_cached_catalog_bars':'data_cache',
 'run_backtest_node':'single_backtest','run_backtest':'single_backtest',
 '_run_single_backtest_task':'slot_execution','_worker_init_ignore_sigint':'slot_execution','_run_single_slot_node':'slot_execution',
 '_run_single_slot':'slot_execution','_extract_slot_from_group_reports':'slot_execution','_run_slot_group_node':'slot_execution','_run_slot_group':'slot_execution',
 '_extract_results':'results','_pick_col':'report_utils','_to_utc_ts':'report_utils','_position_realized_in_base':'results','_position_unrealized_in_base':'results',
 '_row_pnl_to_base':'results','_base_values_from_report':'results','_positions_report_realized_in_base':'results','positions_report_with_base':'results',
 '_positions_pnl_series':'portfolio_results','_per_strategy_breakdown':'portfolio_results','_splice_merged_results':'portfolio_results',
 '_merge_portfolio_results':'portfolio_results','_extract_trade_pnls':'portfolio_results','_extract_portfolio_results':'portfolio_results',
 '_VWAP_SL_PREFIXES':'exit_fill','_VWAP_TP_PREFIXES':'exit_fill','_session_start_minute':'exit_fill','_vwap_session_bucket':'exit_fill',
 '_build_vwap_lookup':'exit_fill','_vwap_normalize_tag':'exit_fill','_vwap_ts_to_ns':'exit_fill','_vwap_row_is_long':'exit_fill',
 '_vwap_adjust_pnl_cell':'exit_fill','_apply_vwap_fill':'exit_fill','_build_close_lookup':'exit_fill','_apply_directional_close_fill':'exit_fill',
 'run_portfolio_backtest':'orchestration',
}

nodes = []
for node in tree.body:
    name = None
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        name = node.name
    elif isinstance(node, ast.Assign):
        for t in node.targets:
            if isinstance(t, ast.Name):
                name = t.id
                break
    elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        name = node.target.id
    if name:
        nodes.append((name, node))

defined = {nm for nm, _ in nodes}
print("UNMAPPED defined:", [n for n in defined if n not in MAP])
print("MAPPED-but-not-found:", [n for n in MAP if n not in defined])

allnames = set(MAP)
edges = set()
for name, node in nodes:
    if name not in MAP:
        continue
    m = MAP[name]
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
            if sub.id in allnames and sub.id != name and MAP[sub.id] != m:
                edges.add((m, MAP[sub.id]))

mods = sorted(set(MAP.values()))
adj = {m: set() for m in mods}
for a, b in sorted(edges):
    adj[a].add(b)
print("\nMODULE EDGES (A imports from B):")
for a, b in sorted(edges):
    print("  %-24s -> %s" % (a, b))

color = {m: 0 for m in mods}
cyc = []
def dfs(u, stack):
    color[u] = 1
    stack.append(u)
    for v in adj[u]:
        if color[v] == 1:
            cyc.append(stack[stack.index(v):] + [v])
        elif color[v] == 0:
            dfs(v, stack)
    stack.pop()
    color[u] = 2
for m in mods:
    if color[m] == 0:
        dfs(m, [])
print("\nTOTAL modules:", len(mods))
print("CYCLES:", cyc if cyc else "NONE - acyclic OK")
