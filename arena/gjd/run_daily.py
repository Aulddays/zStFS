#!/usr/bin/env python3
"""
gjd tracking daily task

Modes:
    python run_daily.py             # one-shot build (online)
    python run_daily.py --test      # one-shot build (mock data, offline)
    python run_daily.py --schedule  # keep running until all data catches up to latest trade date

In schedule mode, the script loops:
  1. determine the latest trade date (from sh000001 daily K)
  2. backfill shares_history and vol5min_history from local latest to target date
  3. regenerate fe/snapshot.json
  4. if everything is up to date, exit; otherwise sleep 10 min and retry
  5. max runtime: 8 hours
"""

import sys
import os
import re
import json
import time
import logging
from datetime import datetime, timedelta
from urllib.request import urlopen, Request

def setup_logging():
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%Y%m%d-%H%M%S", level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.addLevelName(logging.CRITICAL, "CRI")
    logging.addLevelName(logging.ERROR, "ERR")
    logging.addLevelName(logging.WARNING, "WRN")
    logging.addLevelName(logging.INFO, "INF")
    logging.addLevelName(logging.DEBUG, "DBG")

# ----------------------------------------------------------------------
# configs
# ----------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FE_DIR = os.path.join(SCRIPT_DIR, "fe")
SNAPSHOT_FILE = os.path.join(FE_DIR, "snapshot.json")
SNAPSHOT_TEST_FILE = os.path.join(SCRIPT_DIR, "snapshot_test.json")
SHARES_FILE = os.path.join(SCRIPT_DIR, "shares_history.json")
VOL5MIN_FILE = os.path.join(SCRIPT_DIR, "vol5min_history.json")

# MA window length for `shape`
SHAPE_WINDOW = 20

# max runtime for schedule mode (seconds)
MAX_RUNTIME = 14 * 3600
# sleep between rounds (seconds)
SLEEP_INTERVAL = 600
# how many 5min bars to fetch per call (each call returns ~5 trading days of data)
VOL5MIN_BATCH_BARS = 480  # ~10 trading days, enough to cover gaps
# keep vol5min history for N days (sliding window uses 20, with headroom)
VOL5MIN_KEEP_DAYS = 60

# ETFs
# group: core = 2025年报汇金前十大持有人口径有持仓（最后一次实名披露）
# group: watch = 扩展观察，2025年报未见汇金持仓
# huijin_yi: 2025年报中央汇金持仓份额（亿份）
ETF_LIST = [
    {"code": "510300", "tcode": "sh510300", "name": "沪深300ETF华泰柏瑞", "index": "沪深300",   "group": "core",  "huijin_yi": 735.13},
    {"code": "510310", "tcode": "sh510310", "name": "沪深300ETF易方达",   "index": "沪深300",   "group": "core",  "huijin_yi": 558.94},
    {"code": "510050", "tcode": "sh510050", "name": "上证50ETF华夏",      "index": "上证50",    "group": "core",  "huijin_yi": 487.59},
    {"code": "510330", "tcode": "sh510330", "name": "沪深300ETF华夏",     "index": "沪深300",   "group": "core",  "huijin_yi": 420.28},
    {"code": "159919", "tcode": "sz159919", "name": "沪深300ETF嘉实",     "index": "沪深300",   "group": "core",  "huijin_yi": 354.98},
    {"code": "512100", "tcode": "sh512100", "name": "中证1000ETF南方",    "index": "中证1000",  "group": "core",  "huijin_yi": 221.69},
    {"code": "588080", "tcode": "sh588080", "name": "科创50ETF易方达",    "index": "科创50",    "group": "core",  "huijin_yi": 216.07},
    {"code": "159915", "tcode": "sz159915", "name": "创业板ETF易方达",    "index": "创业板",     "group": "core",  "huijin_yi": 170.20},
    {"code": "510500", "tcode": "sh510500", "name": "中证500ETF南方",     "index": "中证500",   "group": "core",  "huijin_yi": 142.17},
    {"code": "159845", "tcode": "sz159845", "name": "中证1000ETF华夏",    "index": "中证1000",  "group": "core",  "huijin_yi": 137.26},
    {"code": "560010", "tcode": "sh560010", "name": "中证1000ETF广发",    "index": "中证1000",  "group": "core",  "huijin_yi": 105.50},
    {"code": "510180", "tcode": "sh510180", "name": "上证180ETF华安",     "index": "上证180",   "group": "core",  "huijin_yi":  52.32},
    {"code": "159629", "tcode": "sz159629", "name": "中证1000ETF富国",    "index": "中证1000",  "group": "core",  "huijin_yi":  36.34},
    {"code": "510230", "tcode": "sh510230", "name": "金融ETF国泰",        "index": "中证金融",   "group": "core",  "huijin_yi":  25.64},
    {"code": "159922", "tcode": "sz159922", "name": "中证500ETF嘉实",     "index": "中证500",   "group": "core",  "huijin_yi":  23.11},
    {"code": "512500", "tcode": "sh512500", "name": "中证500ETF华夏",     "index": "中证500",   "group": "core",  "huijin_yi":  20.51},
    {"code": "515800", "tcode": "sh515800", "name": "中证800ETF汇添富",   "index": "中证800",   "group": "core",  "huijin_yi":  13.08},
    {"code": "159952", "tcode": "sz159952", "name": "创业板ETF广发",      "index": "创业板",     "group": "core",  "huijin_yi":  10.91},
    {"code": "588050", "tcode": "sh588050", "name": "科创50ETF工银",      "index": "科创50",    "group": "core",  "huijin_yi":   9.70},
    {"code": "159901", "tcode": "sz159901", "name": "深证100ETF易方达",   "index": "深证100",   "group": "core",  "huijin_yi":   7.19},
    {"code": "588000", "tcode": "sh588000", "name": "科创50ETF华夏",      "index": "科创50",    "group": "watch", "huijin_yi":   0.0},
    {"code": "512050", "tcode": "sh512050", "name": "A500ETF华夏",        "index": "中证A500",  "group": "watch", "huijin_yi":   0.0},
    {"code": "159338", "tcode": "sz159338", "name": "中证A500ETF国泰",    "index": "中证A500",  "group": "watch", "huijin_yi":   0.0},
    {"code": "159339", "tcode": "sz159339", "name": "A500ETF银华",        "index": "中证A500",  "group": "watch", "huijin_yi":   0.0},
]

# Empirical intraday turnover distribution for A-share ETFs (48 5-min points, cumulative ratio)
# High at open -> gradual decline -> low before noon -> rebound in afternoon -> highest at close
# Used as fallback when historical data is insufficient
DEFAULT_SHAPE = [
    ["09:35", 0.062], ["09:40", 0.097], ["09:45", 0.128], ["09:50", 0.154],
    ["09:55", 0.178], ["10:00", 0.200], ["10:05", 0.219], ["10:10", 0.237],
    ["10:15", 0.253], ["10:20", 0.268], ["10:25", 0.283], ["10:30", 0.297],
    ["10:35", 0.310], ["10:40", 0.322], ["10:45", 0.334], ["10:50", 0.345],
    ["10:55", 0.356], ["11:00", 0.366], ["11:05", 0.375], ["11:10", 0.385],
    ["11:15", 0.394], ["11:20", 0.403], ["11:25", 0.413], ["11:30", 0.425],
    ["13:05", 0.445], ["13:10", 0.461], ["13:15", 0.476], ["13:20", 0.490],
    ["13:25", 0.504], ["13:30", 0.517], ["13:35", 0.531], ["13:40", 0.545],
    ["13:45", 0.558], ["13:50", 0.571], ["13:55", 0.584], ["14:00", 0.597],
    ["14:05", 0.611], ["14:10", 0.625], ["14:15", 0.639], ["14:20", 0.654],
    ["14:25", 0.669], ["14:30", 0.685], ["14:35", 0.703], ["14:40", 0.723],
    ["14:45", 0.746], ["14:50", 0.785], ["14:55", 0.895], ["15:00", 1.000],
]


# ----------------------------------------------------------------------
# main entry
# ----------------------------------------------------------------------

def main(argv):
    setup_logging()

    # test mode: short-circuit, mock data, no history writes
    if len(argv) > 1 and argv[1] in ("--test", "-t"):
        build_snapshot(test_mode=True, save_history=False)
        return 0

    schedule_mode = len(argv) > 1 and argv[1] == "--schedule"
    if schedule_mode:
        logging.info("=== Schedule mode start, max runtime=%dh ===", MAX_RUNTIME // 3600)

    shares_history = load_json(SHARES_FILE, {})
    vol5min_history = load_json(VOL5MIN_FILE, {})

    tm_start = time.time()
    round_num = 0

    while True:
        round_num += 1
        if schedule_mode:
            logging.info("--- Round %d ---", round_num)

        target_date = get_latest_trade_date()
        logging.info("Latest trade date: %s", target_date)

        shares_changed, shares_done = update_shares_history(shares_history, target_date)
        vol5min_changed, vol5min_done = update_vol5min_history(vol5min_history, target_date)

        # only rebuild snapshot when history data changes (running post-market, realtime data is static)
        if shares_changed or vol5min_changed or round_num == 1:
            build_snapshot(test_mode=False, shares_history=shares_history,
                           vol5min_history=vol5min_history, save_history=False)

        if shares_changed:
            save_json(SHARES_FILE, shares_history)
            logging.info("Shares history saved: %s (%d days)", SHARES_FILE, len(shares_history))
        if vol5min_changed:
            save_json(VOL5MIN_FILE, vol5min_history)
            logging.info("vol5min history saved: %s (%d days)", VOL5MIN_FILE, len(vol5min_history))

        # one-shot mode: done after first round
        if not schedule_mode:
            break

        # schedule mode: check if done
        logging.info("Progress: shares=%s vol5min=%s",
                     "DONE" if shares_done else "pending",
                     "DONE" if vol5min_done else "pending")
        if shares_done and vol5min_done:
            logging.info("All data up to date. Exiting.")
            break

        if time.time() - tm_start > MAX_RUNTIME:
            logging.warning("Reached max runtime (%dh). Exiting.", MAX_RUNTIME // 3600)
            break

        logging.info("Sleep %ds...", SLEEP_INTERVAL)
        time.sleep(SLEEP_INTERVAL)

    return 0


# ----------------------------------------------------------------------
# build snapshot
# ----------------------------------------------------------------------

# cache for market data used by build_snapshot, stored once confirmed complete
_snap_data_cache = {
    "realtime": None,   # {code: {...}} — all ETFs from tencent
    "daily": {},        # tcode -> [daily bars...] — sina daily K
    "5min": {},         # tcode -> [5min bars...] — sina 5min K
}


def build_snapshot(test_mode=False, shares_history=None, vol5min_history=None, save_history=True):
    """
    Build snapshot and save to fe/snapshot.json.

    If shares_history / vol5min_history are provided, use them instead of loading from disk.
    If save_history is False, skip writing shares/vol5min files (caller handles saving).
    Market data (realtime, daily K, 5min K) is cached globally once confirmed complete.
    """
    logging.info("Building snapshot (test_mode=%s) ...", test_mode)

    os.makedirs(FE_DIR, exist_ok=True)
    trade_date = datetime.now().strftime("%Y%m%d")
    tcodes = [e["tcode"] for e in ETF_LIST]

    # load history if not provided
    if shares_history is None:
        shares_history = load_json(SHARES_FILE, {})
    if vol5min_history is None:
        vol5min_history = load_json(VOL5MIN_FILE, {})
    prev_date = prev_trading_date(trade_date)

    # --- realtime quotes ---
    if test_mode:
        logging.info("Test mode: using mock data")
        realtime = _mock_realtime()
    elif _snap_data_cache["realtime"] is not None:
        realtime = _snap_data_cache["realtime"]
        logging.info("Using cached realtime quotes (%d ETFs)", len(realtime))
    else:
        logging.info("Fetching realtime quotes (%d ETFs) ...", len(tcodes))
        realtime = fetch_tencent_realtime(tcodes)
        logging.info("Got %d realtime entries", len(realtime))
        # cache if post-market (data won't change anymore)
        if datetime.now().strftime("%H%M") >= "1500":
            _snap_data_cache["realtime"] = realtime
            logging.info("Cached realtime quotes (post-market)")

    # --- build per-ETF items ---
    items = []
    today_vol5min = {}  # {code: [[time, amount_yi], ...]}  today's raw 5min bars (for tail_ratio)

    for etf in ETF_LIST:
        code = etf["code"]
        tcode = etf["tcode"]

        if test_mode:
            daily = _mock_daily(code)
            bars_5min = []
        else:
            # daily K-line: use cache if complete, otherwise fetch and check completeness
            if tcode in _snap_data_cache["daily"]:
                daily = _snap_data_cache["daily"][tcode]
            else:
                daily = fetch_sina_daily(tcode, days=40)
                # cache if latest bar is today or later (data is complete for our purposes)
                if daily:
                    last_date = daily[-1]["date"].replace("-", "")
                    if last_date >= trade_date or datetime.now().strftime("%H%M") >= "1500":
                        _snap_data_cache["daily"][tcode] = daily

            # 5min K-line: use cache if complete
            if tcode in _snap_data_cache["5min"]:
                bars_5min = _snap_data_cache["5min"][tcode]
            else:
                bars_5min = fetch_sina_5min(tcode)
                # cache if today's data is complete (near full 48 bars)
                if len(bars_5min) >= 45:
                    _snap_data_cache["5min"][tcode] = bars_5min

        # today's raw intraday data [[time, amount_yi], ...]
        today_amounts = [[b["time"], b["amount_yi"]] for b in bars_5min] if bars_5min else []
        # compute today's shape (cumulative turnover ratio)
        today_shape = build_shape_from_amounts(today_amounts) if today_amounts else []

        # shape MA: take last SHAPE_WINDOW days from vol5min_history, compute shape on the fly
        shape_hist_list = []
        sorted_dates = sorted(vol5min_history.keys())
        for d in sorted_dates[-SHAPE_WINDOW:]:
            if code in vol5min_history[d]:
                shape_hist_list.append(build_shape_from_amounts(vol5min_history[d][code]))
        # use real average if >= 5 days of history, otherwise fall back to empirical default
        if len(shape_hist_list) >= 5:
            shape_avg = avg_shapes(shape_hist_list)
        else:
            shape_avg = [list(p) for p in DEFAULT_SHAPE]

        item = build_item(etf, realtime.get(code), daily, shape_avg)

        # today's tail ratio (only filled when data is complete, typically after all 48 bars at close)
        if today_shape and len(today_shape) >= 45:
            item["tail_ratio"] = calc_tail_ratio(today_shape)
            today_vol5min[code] = today_amounts
        else:
            item["tail_ratio"] = None

        # tail ratio baseline: 20-day historical average
        if shape_hist_list:
            tail_vals = []
            for s in shape_hist_list:
                r = calc_tail_ratio(s)
                if r is not None:
                    tail_vals.append(r)
            if tail_vals:
                item["tail_ratio_base"] = round(sum(tail_vals) / len(tail_vals), 4)
            else:
                item["tail_ratio_base"] = 0.2906
        else:
            item["tail_ratio_base"] = 0.2906

        items.append(item)
        logging.info("  %s %s: amount=%.2f亿 avg20=%.2f亿 shape_hist=%d天",
                     code, etf["name"], item["amount_yi"], item["avg20_yi"],
                     len(shape_hist_list))

    # all ETFs in shares_history, sorted by date
    # shares_history format: {date: {code: [close_price, shares_yi]}}
    # snapshot's shares_history keeps just shares_yi numbers (frontend chart only needs shares)
    all_codes = [e["code"] for e in ETF_LIST]
    shares_clean = {}
    for d in sorted(shares_history.keys()):
        row = shares_history[d]
        shares_clean[d] = {c: round(row[c][1], 4) for c in all_codes if c in row}

    share_dates = sorted(shares_clean.keys())
    latest_share_date = share_dates[-1] if share_dates else ""
    # prev_share_date = the second-to-last in shares_history (not prev calendar day)
    # because SSE data may not be updated yet on the day after close
    prev_share_date = share_dates[-2] if len(share_dates) >= 2 else ""

    # compute share-related fields per ETF
    # each ETF may have different latest date (when SZSE/SSE update at different paces)
    for item in items:
        code = item["code"]
        # find the two most recent dates with data for this ETF, scanning backward
        last_d = None
        prev_d = None
        for d in reversed(share_dates):
            if code in shares_history.get(d, {}):
                if last_d is None:
                    last_d = d
                else:
                    prev_d = d
                    break
        if last_d and prev_d:
            last_entry = shares_history[last_d][code]  # [close_price, shares_yi]
            prev_entry = shares_history[prev_d][code]
            last_sh_yi = last_entry[1]
            prev_sh_yi = prev_entry[1]
            # use official precise value from shares_history (not Tencent realtime estimate)
            item["shares"] = last_sh_yi * 1e8
            item["shares_yi"] = last_sh_yi
            item["prev_shares"] = prev_sh_yi * 1e8
            item["share_date"] = last_d
            item["prev_share_date"] = prev_d
            # market cap diff = shares_delta × close_price (from shares_history)
            close_price = last_entry[0] if last_entry[0] is not None else (item["price"] or 0)
            diff = (last_sh_yi - prev_sh_yi) * close_price
            item["subscr_net_yi"] = round(diff, 2)
        elif last_d:
            item["prev_shares"] = None
            item["share_date"] = last_d
            item["prev_share_date"] = ""
            item["subscr_net_yi"] = None
        else:
            item["prev_shares"] = None
            item["share_date"] = ""
            item["prev_share_date"] = ""
            item["subscr_net_yi"] = None

        # 30-day shares + price series for the per-row sparkline-like chart
        # format: [[date_yyyymmdd, close_price, shares_yi], ...] oldest first
        series_30 = []
        for d in share_dates:
            if code in shares_history.get(d, {}):
                entry = shares_history[d][code]  # [price, shares_yi]
                series_30.append([d, entry[0], entry[1]])
        item["shares_30d"] = series_30[-30:]

    # --- assemble snapshot ---
    snapshot = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": trade_date,
        "prev_share_date": prev_share_date,
        "latest_share_date": latest_share_date,
        "items": items,
        "shares_history": shares_clean,
    }

    # --- save ---
    out_file = SNAPSHOT_TEST_FILE if test_mode else SNAPSHOT_FILE
    save_json(out_file, snapshot)
    logging.info("Snapshot saved: %s", out_file)

    if save_history:
        save_json(SHARES_FILE, shares_history)
        logging.info("Shares history saved: %s (%d days)", SHARES_FILE, len(shares_history))

        if not test_mode:
            save_json(VOL5MIN_FILE, vol5min_history)
            logging.info("vol5min history saved: %s (%d days)", VOL5MIN_FILE, len(vol5min_history))

    logging.info("Done.")
    return snapshot


# ----------------------------------------------------------------------
# history backfill
# ----------------------------------------------------------------------

def _etf_latest_date(shares_history, code):
    """Find the latest date that has data for a given ETF code."""
    for d in sorted(shares_history.keys(), reverse=True):
        if code in shares_history[d]:
            return d
    return ""


def update_shares_history(shares_history, target_date):
    """
    Backfill shares_history up to target_date, per-ETF.
    Each entry stores [close_price, shares_yi].
    Logic: first fetch shares from exchange, then fetch daily close from Sina.
    Only save when BOTH price and shares are available; if incomplete, treat as not fetched.
    Returns (changed, done): whether data was updated, and whether all ETFs reached target_date.
    """
    sse_codes = [e["code"] for e in ETF_LIST if e["tcode"].startswith("sh")]
    szse_codes = [e["code"] for e in ETF_LIST if e["tcode"].startswith("sz")]

    # find the oldest "latest date" per market — determines the range we need to fetch
    def oldest_latest(codes):
        oldest = target_date
        for code in codes:
            ld = _etf_latest_date(shares_history, code)
            if ld < oldest:
                oldest = ld
        return oldest

    sse_oldest = oldest_latest(sse_codes) if sse_codes else target_date
    szse_oldest = oldest_latest(szse_codes) if szse_codes else target_date

    # if all ETFs in both markets are already at or past target_date, nothing to do
    if sse_oldest >= target_date and szse_oldest >= target_date:
        return False, True

    # --- Step 1: fetch shares data into temp structure ---
    # new_shares_raw: {date_str: {code: shares_yi}}
    new_shares_raw = {}

    # --- SZSE: batch fetch from oldest lagging date to target ---
    if szse_codes and szse_oldest < target_date:
        szse_start = szse_oldest if szse_oldest else "20200101"
        start_dt = datetime.strptime(szse_start, "%Y%m%d") + timedelta(days=1)
        end_dt = datetime.strptime(target_date, "%Y%m%d")
        if start_dt <= end_dt:
            logging.info("Fetching SZSE shares: %s -> %s",
                         start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
            batch_dt = start_dt
            batch_num = 0
            while batch_dt <= end_dt:
                batch_end = batch_dt + timedelta(days=150)
                if batch_end > end_dt:
                    batch_end = end_dt
                batch_num += 1
                s = batch_dt.strftime("%Y%m%d")
                e = batch_end.strftime("%Y%m%d")
                logging.info("  batch %d: %s ~ %s", batch_num, s, e)
                batch_data = fetch_szse_shares_range(s, e)
                for d, row in batch_data.items():
                    filtered = {c: row[c] for c in szse_codes if c in row}
                    if filtered:
                        if d not in new_shares_raw:
                            new_shares_raw[d] = {}
                        new_shares_raw[d].update(filtered)
                batch_dt = batch_end + timedelta(days=1)

    # --- SSE: day by day, from oldest lagging date to target ---
    if sse_codes and sse_oldest < target_date:
        sse_start = sse_oldest if sse_oldest else "20200101"
        start_dt = datetime.strptime(sse_start, "%Y%m%d") + timedelta(days=1)
        end_dt = datetime.strptime(target_date, "%Y%m%d")
        fill_dates = []
        d = start_dt
        while d <= end_dt:
            if d.weekday() < 5:
                fill_dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)

        if fill_dates:
            logging.info("Fetching SSE shares: %s -> %s (%d weekdays)",
                         fill_dates[0], fill_dates[-1], len(fill_dates))
            for d in fill_dates:
                sse_data = fetch_sse_shares(d)
                if not sse_data:
                    continue
                filtered = {c: sse_data[c] for c in sse_codes if c in sse_data}
                if filtered:
                    if d not in new_shares_raw:
                        new_shares_raw[d] = {}
                    new_shares_raw[d].update(filtered)

    # if no shares data at all, nothing to do
    if not new_shares_raw:
        logging.info("  No new shares data fetched")
        sse_done = (not sse_codes) or oldest_latest(sse_codes) >= target_date
        szse_done = (not szse_codes) or oldest_latest(szse_codes) >= target_date
        return False, sse_done and szse_done

    # --- Step 2: fetch daily close prices from Sina for ETFs that have new shares ---
    # collect all ETF codes that appear in new_shares_raw
    codes_with_new_shares = set()
    for d, row in new_shares_raw.items():
        codes_with_new_shares.update(row.keys())

    # price_by_code: {code: {date_str_yyyymmdd: close_price}}
    price_by_code = {}
    for etf in ETF_LIST:
        code = etf["code"]
        if code not in codes_with_new_shares:
            continue
        tcode = etf["tcode"]
        logging.info("  Fetching daily K for %s (%s)", code, tcode)
        daily = fetch_sina_daily(tcode, days=80)  # enough headroom for backfill
        if not daily:
            logging.warning("  No daily data for %s", code)
            continue
        prices = {}
        for bar in daily:
            d = bar["date"].replace("-", "")
            prices[d] = bar["close"]
        price_by_code[code] = prices

    # --- Step 3: merge — only save entries with both shares and price ---
    changed = False
    merged_count = 0
    for d, row in new_shares_raw.items():
        for code, shares_yi in row.items():
            # skip if already in history
            if d in shares_history and code in shares_history[d]:
                continue
            # need both shares and price
            price = price_by_code.get(code, {}).get(d)
            if price is None:
                continue  # incomplete, skip
            if d not in shares_history:
                shares_history[d] = {}
            shares_history[d][code] = [round(price, 4), round(shares_yi, 4)]
            changed = True
            merged_count += 1

    logging.info("  Merged %d new entries (price + shares)", merged_count)

    # re-check after fetch: did all ETFs catch up?
    sse_done = (not sse_codes) or oldest_latest(sse_codes) >= target_date
    szse_done = (not szse_codes) or oldest_latest(szse_codes) >= target_date
    return changed, sse_done and szse_done


def update_vol5min_history(vol5min_history, target_date):
    """
    Backfill vol5min_history up to target_date.
    Sina 5min API returns recent ~10 trading days with one call (datalen=480),
    so we fetch each ETF once and fill in any missing days.
    Only complete days (>=45 bars) are stored.
    Returns (changed, done): whether data was added, and whether all ETFs have target_date.
    """
    changed = False
    new_days = set()

    for etf in ETF_LIST:
        code = etf["code"]
        tcode = etf["tcode"]
        bars = fetch_sina_5min_range(tcode, datalen=VOL5MIN_BATCH_BARS)
        if not bars:
            continue

        # group bars by date
        by_date = {}  # {date_str: [[time, amount_yi], ...]}
        for bar in bars:
            d = bar["date"]  # YYYYMMDD
            if d > target_date:
                continue
            if d not in by_date:
                by_date[d] = []
            by_date[d].append([bar["time"], bar["amount_yi"]])

        for d, amounts in by_date.items():
            # only keep complete days (near full 48 bars)
            if len(amounts) < 45:
                continue
            if d not in vol5min_history:
                vol5min_history[d] = {}
            if code not in vol5min_history[d]:
                vol5min_history[d][code] = amounts
                changed = True
                new_days.add(d)

    # trim old days
    all_dates = sorted(vol5min_history.keys())
    if len(all_dates) > VOL5MIN_KEEP_DAYS:
        for d in all_dates[:-VOL5MIN_KEEP_DAYS]:
            del vol5min_history[d]

    if new_days:
        logging.info("  vol5min: added data for %d days (%s ... %s)",
                     len(new_days), min(new_days), max(new_days))

    # done: every ETF has complete target_date data
    day_data = vol5min_history.get(target_date, {})
    done = all(etf["code"] in day_data for etf in ETF_LIST)
    return changed, done


def get_latest_trade_date():
    """
    Determine the latest trading date by checking sh000001 daily K-line.
    If current time is before 15:30, consider yesterday the latest (today not yet finished).
    Returns date string YYYYMMDD.
    """
    now = datetime.now()
    end_dt = now
    if now.strftime("%H%M") < "1530":
        end_dt = now - timedelta(days=1)  # today not finished yet
    start_dt = end_dt - timedelta(days=10)
    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    try:
        import akshare as ak
        df = ak.stock_zh_a_daily(symbol="sh000001", start_date=start_date,
                                 end_date=end_date, adjust="")
        if df is None or df.empty:
            logging.warning("get_latest_trade_date: no data, fallback to end_date")
            return end_date
        last_date = str(df.iloc[-1]["date"])[:10].replace("-", "")
        return last_date
    except Exception as e:
        logging.warning("get_latest_trade_date failed: %s, fallback to end_date", e)
        return end_date


# ----------------------------------------------------------------------
# per-ETF item builder
# ----------------------------------------------------------------------

def build_item(etf, realtime_data, daily_kline, shape_avg):
    rt = realtime_data or {}
    price = rt.get("price", 0)
    prev_close = rt.get("prev_close", 0)
    pct = rt.get("pct", 0)
    amount_wan = rt.get("amount_wan", 0)
    amount_yi = rt.get("amount_yi", 0)
    outer_lots = rt.get("outer_lots", 0)
    inner_lots = rt.get("inner_lots", 0)
    turnover = rt.get("turnover", 0)
    mktcap_yi = rt.get("mktcap_yi", 0)
    shares = rt.get("shares", 0)
    shares_yi = rt.get("shares_yi", 0)
    ts = rt.get("ts", "")
    date = rt.get("date", "")

    # hist: compact daily K-line of last 30 days
    hist = []
    if daily_kline:
        for bar in daily_kline:
            hist.append({
                "date": bar["date"],
                "amount_yi": bar["amount_yi"],
                "volume_lots": bar["volume_lots"],
            })
    hist = hist[-30:]

    avg20 = calc_avg20(hist) if hist else 0

    return {
        "code": etf["code"],
        "tcode": etf["tcode"],
        "name": etf["name"],
        "index": etf["index"],
        "group": etf["group"],
        "huijin_yi": etf["huijin_yi"],
        "price": price,
        "prev_close": prev_close,
        "pct": pct,
        "amount_yi": amount_yi,
        "amount_wan": amount_wan,
        "mktcap_yi": mktcap_yi,
        "shares": shares,
        "shares_yi": shares_yi,
        "outer_lots": outer_lots,
        "inner_lots": inner_lots,
        "turnover": turnover,
        "ts": ts,
        "date": date,
        "avg20_yi": avg20,
        "shape": shape_avg,
        "hist": hist,
        "tail_ratio": None,
        "tail_ratio_base": 0.2906,  # empirical value, to be replaced with historical data
        "subscr_net_yi": None,
        "prev_shares": None,
    }


# ----------------------------------------------------------------------
# calculations
# ----------------------------------------------------------------------

def calc_avg20(hist):
    """ MA-20 of daily trading value (亿) """
    if not hist:
        return 0.0
    recent = hist[-20:]
    amounts = [d["amount_yi"] for d in recent if d.get("amount_yi", 0) > 0]
    if not amounts:
        return 0.0
    return round(sum(amounts) / len(amounts), 2)


def build_shape_from_amounts(amounts):
    """
    Build cumulative turnover ratio curve from raw 5min amount data.
    amounts: [[time, amount_yi], ...]
    Returns [[hhmm, ratio], ...]
    """
    if not amounts:
        return []
    total = sum(a[1] for a in amounts)
    if total <= 0:
        return []

    cumulative = 0.0
    shape = []
    for time_str, amt in amounts:
        cumulative += amt
        ratio = min(1.0, cumulative / total)
        shape.append([time_str, round(ratio, 5)])
    return shape


def avg_shapes(shape_list):
    """Average shape curves across multiple days"""
    if not shape_list:
        return []
    times = [p[0] for p in shape_list[0]]
    avg = []
    for i, t in enumerate(times):
        vals = [s[i][1] for s in shape_list if i < len(s)]
        if vals:
            avg.append([t, round(sum(vals) / len(vals), 5)])
    return avg


def calc_tail_ratio(shape, daily_amount=None):
    """
    Compute the tail-half-hour (14:30-15:00) share of full-day turnover from shape curve.
    shape is a cumulative ratio curve; tail ratio = 1 - shape(point before 14:30)
    """
    if not shape:
        return None
    # find the nearest point before 14:30
    for i, (t, r) in enumerate(shape):
        if t >= "14:30":
            before = shape[i - 1][1] if i > 0 else 0
            return round(1.0 - before, 4)
    return None


def prev_trading_date(date_str):
    """Roughly roll back to the previous weekday (holidays not considered)"""
    d = datetime.strptime(date_str, "%Y%m%d")
    while True:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")


# ----------------------------------------------------------------------
# data fetchers
# ----------------------------------------------------------------------

def _http_get(url, timeout=10, referer=None):
    headers = {"User-Agent": "Mozilla/5.0"}
    if referer:
        headers["Referer"] = referer
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            for enc in ["utf-8", "gbk", "gb2312"]:
                try:
                    return raw.decode(enc)
                except UnicodeDecodeError:
                    continue
            return raw.decode("utf-8", errors="replace")
    except Exception as e:
        logging.warning("fetch fail: %s (%s)", url, e)
        return None


def fetch_tencent_realtime(tcodes):
    """
    batch call tx realtime {code: {price, prev_close, pct, amount_wan,
    amount_yi, outer_lots, inner_lots, turnover, mktcap_yi, shares, shares_yi, ts, date}}
    """
    if not tcodes:
        return {}
    url = f"https://qt.gtimg.cn/q={','.join(tcodes)}&r={int(datetime.now().timestamp())}"
    text = _http_get(url)
    if not text:
        return {}

    result = {}
    pattern = re.compile(r'v_(\w+)="([^"]*)"')
    for m in pattern.finditer(text):
        body = m.group(2)
        a = body.split("~")
        if len(a) < 40:
            continue

        def _n(i):
            try:
                return float(a[i])
            except (ValueError, IndexError):
                return 0.0

        code = a[2] if len(a) > 2 else ""
        shares = _n(72) if len(a) > 72 else 0.0
        ts = a[30] if len(a) > 30 else ""
        date = ts[:8] if len(ts) >= 8 else ""

        result[code] = {
            "price": _n(3),
            "prev_close": _n(4),
            "pct": _n(32),
            "amount_wan": _n(37),
            "amount_yi": round(_n(37) / 1e4, 4),
            "outer_lots": _n(7),
            "inner_lots": _n(8),
            "turnover": _n(38),
            "mktcap_yi": _n(45),
            "shares": shares,
            "shares_yi": round(shares / 1e8, 4),
            "ts": ts,
            "date": date,
        }
    return result


def fetch_sse_shares(date_str):
    """
    fetch one-day SSE-official ETF share data  (using akshare)
    returns {code: shares_yi}  (亿份), {} on failure
    """
    try:
        import akshare as ak
        df = ak.fund_etf_scale_sse(date=date_str)
        result = {}
        for _, row in df.iterrows():
            code = str(row["基金代码"]).strip()
            shares = float(row["基金份额"])  # 单位: 份
            result[code] = round(shares / 1e8, 4)  # 转亿份
        return result
    except Exception as e:
        logging.warning("SSE shares %s failed: %s", date_str, e)
        return {}


def fetch_szse_shares(date_str):
    """
    fetch one-day SZSE-official ETF share data  (using akshare)
    returns {code: shares_yi}  (亿份), {} on failure
    """
    result_by_date = fetch_szse_shares_range(date_str, date_str)
    return result_by_date.get(date_str, {})


def fetch_szse_shares_range(start_date, end_date):
    """
    fetch SZSE-official ETF share data for a date range (using akshare).
    returns {date_str: {code: shares_yi}}, {} on failure
    """
    try:
        import akshare as ak
        df = ak.fund_scale_daily_szse(
            start_date=start_date, end_date=end_date, symbol="ETF"
        )
        result = {}
        for _, row in df.iterrows():
            d = str(row["日期"]).replace("-", "")
            code = str(row["基金代码"]).strip()
            shares = float(row["基金份额"])  # 单位: 份
            if d not in result:
                result[d] = {}
            result[d][code] = round(shares / 1e8, 4)
        return result
    except Exception as e:
        logging.warning("SZSE shares %s~%s failed: %s", start_date, end_date, e)
        return {}


def fetch_sina_daily(tcode, days=40):
    """
    sina daily K-line (qfq, with volume data).
    return: [{date, open, close, high, low, volume_lots, amount_yi}, ...]
    In ascending order by date.
    """
    try:
        import akshare as ak
        df = ak.fund_etf_hist_sina(symbol=tcode)
    except Exception as e:
        logging.warning("sina daily %s failed: %s", tcode, e)
        return []

    if df is None or df.empty:
        return []

    result = []
    for _, row in df.tail(days).iterrows():
        try:
            date_str = str(row["date"])[:10]  # YYYY-MM-DD
            volume = float(row["volume"])  # in 手
            amount = float(row["amount"]) / 1e8  # 元 -> 亿元
            result.append({
                "date": date_str,
                "open": float(row["open"]),
                "close": float(row["close"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "volume_lots": volume,
                "amount_yi": round(amount, 4),
            })
        except (ValueError, KeyError):
            continue

    return result


def fetch_sina_5min(tcode):
    """
    sina 5min K-line for today via CN_MarketDataService.getKLineData
    return: [{time, open, close, high, low, volume_lots, amount_yi}, ...]
    Only today's bars are returned; data is already in 5-min intervals.
    """
    all_bars = fetch_sina_5min_range(tcode, datalen=48)
    today_str = datetime.now().strftime("%Y%m%d")
    return [b for b in all_bars if b["date"] == today_str]


def _parse_sina_5min_jsonp(text):
    """Parse JSONP response from sina getKLineData into raw list."""
    if not text:
        return []
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < 0 or end <= start:
        return []
    text = text[start:end + 1]
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return data


def fetch_sina_5min_range(tcode, datalen=480):
    """
    sina 5min K-line for multiple recent days.
    datalen: number of 5min bars to fetch (48 per trading day)
    returns [{date(YYYYMMDD), time(HH:MM), open, close, high, low, volume_lots, amount_yi}, ...]
    sorted ascending by date+time.
    """
    url = (
        f"https://quotes.sina.cn/cn/api/jsonp_v2.php/=/"
        f"CN_MarketDataService.getKLineData?symbol={tcode}&scale=5&ma=no&datalen={datalen}"
    )
    text = _http_get(url, referer="https://finance.sina.com.cn/")
    data = _parse_sina_5min_jsonp(text)
    if not data:
        return []

    result = []
    for row in data:
        try:
            day_str = str(row.get("day", ""))
            if " " in day_str:
                date_part, time_part = day_str.split(" ", 1)
                hhmm = time_part[:5]
            else:
                date_part = day_str
                hhmm = ""
            date_yyyymmdd = date_part.replace("-", "")
            volume = float(row.get("volume", 0))
            amount = float(row.get("amount", 0))
            result.append({
                "date": date_yyyymmdd,
                "time": hhmm,
                "open": float(row.get("open", 0)),
                "close": float(row.get("close", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "volume_lots": volume,
                "amount_yi": round(amount / 1e8, 6),
            })
        except (ValueError, TypeError):
            continue

    return result


# ----------------------------------------------------------------------
# utils
# ----------------------------------------------------------------------

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.warning("load %s failed: %s", path, e)
        return default


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False)


# ----------------------------------------------------------------------
# test mode: mock data
# ----------------------------------------------------------------------

def _mock_realtime():
    import random
    random.seed(42)
    result = {}
    for etf in ETF_LIST:
        price = round(random.uniform(1.5, 5.5), 3)
        prev_close = round(price * random.uniform(0.98, 1.02), 3)
        pct = round((price - prev_close) / prev_close * 100, 2)
        amount_yi = round(random.uniform(5, 120), 2)
        amount_wan = amount_yi * 1e4
        outer = random.randint(1_000_000, 8_000_000)
        inner = random.randint(1_000_000, 8_000_000)
        turnover = round(random.uniform(0.5, 5.0), 2)
        mktcap = round(random.uniform(50, 2000), 2)
        shares_yi = round(random.uniform(20, 800), 2)
        shares = shares_yi * 1e8
        result[etf["code"]] = {
            "price": price, "prev_close": prev_close, "pct": pct,
            "amount_wan": amount_wan, "amount_yi": amount_yi,
            "outer_lots": float(outer), "inner_lots": float(inner),
            "turnover": turnover, "mktcap_yi": mktcap,
            "shares": shares, "shares_yi": shares_yi,
            "ts": datetime.now().strftime("%Y%m%d%H%M%S"),
            "date": datetime.now().strftime("%Y%m%d"),
        }
    return result


def _mock_daily(code):
    import random
    random.seed(hash(code) % (2**31))
    result = []
    base_price = random.uniform(1.5, 5.5)
    base_amount = random.uniform(5, 80)
    today = datetime.now()
    for i in range(45, 0, -1):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        change = random.gauss(0, 0.015)
        close = round(base_price * (1 + change), 3)
        open_p = round(close * random.uniform(0.99, 1.01), 3)
        high = round(max(open_p, close) * random.uniform(1.0, 1.015), 3)
        low = round(min(open_p, close) * random.uniform(0.985, 1.0), 3)
        volume = random.randint(1_000_000, 15_000_000)
        amount = round(base_amount * random.uniform(0.6, 1.8), 2)
        result.append({
            "date": d.strftime("%Y-%m-%d"),
            "open": open_p, "close": close, "high": high, "low": low,
            "volume_lots": float(volume), "amount_yi": amount,
        })
        base_price = close
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv))
