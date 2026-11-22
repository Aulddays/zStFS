#!/usr/bin/env python3
"""
Intraday 1-min realtime watcher for arbitrary stock codes.

Fetch data from Tencent API every 10 sec (aligned to :05/:15/...).
Aggregate into 1-min bars:
    `bars[code] = [[price, outer_lots, inner_lots], ...]`

Write to both zstore and memcache.

Usage:
    python watch_realtime.py watchlist.jsonl

watchlist.jsonl format (one per line):
    {"code": "sh510300"}
    {"code": "sz399001"}

The watchlist file is reloaded automatically when modified.
"""

import sys
import os
import re
import json
import time
import signal
import logging
import threading
import queue
from dotenv import load_dotenv
from urllib.request import urlopen, Request
from pymemcache.client import base as _pymemcache_base

import cna_calendar
import common
from zstorec import zstore


# ----------------------------------------------------------------------
# configs
# ----------------------------------------------------------------------

FETCH_INTERVAL = 20       # seconds between fetches
FETCH_ALIGN_SEC = 7       # align to :07/:17/:27/... second boundaries

MORNING_START_SEC = 9 * 3600 + 30 * 60     # 09:30:00
MORNING_END_SEC = 11 * 3600 + 30 * 60     # 11:30:00
AFTERNOON_START_SEC = 13 * 3600           # 13:00:00
AFTERNOON_END_SEC = 15 * 3600             # 15:00:00

CELL_SECONDS = 60         # 1 min per cell
CELLS_PER_SESSION = 120   # 120 min / 1 min
TOTAL_CELLS = CELLS_PER_SESSION * 2  # 240 cells per day
assert(4 * 3600 == CELL_SECONDS * TOTAL_CELLS)

STORE_INTERVAL = 60       # seconds between zstore writes
ZSTORE_TABLE = "zStFS"
ZSTORE_PARTKEY = "watch-1m"
MEMCACHE_KEY_PREFIX = "zStFS.watch.realtime."

load_dotenv()
ZSTORE_URL = os.environ.get("ZSTORE_URL", None)
ZSTORE_AUTHKEY = os.environ.get("ZSTORE_AUTHKEY", None)
MEMCACHE_ADDR = os.environ.get("MEMCACHE_ADDR", None)


def main(argv):
    common.setup()

    if len(argv) < 2:
        print("Usage: python watch_realtime.py <watchlist.jsonl>", file=sys.stderr)
        print("  e.g. python watch_realtime.py watchlist.jsonl", file=sys.stderr)
        return 1

    watchlist_file = argv[1]

    if not cna_calendar.isopen():
        logging.info("Today is not a trading day, exit.")
        return 0

    logging.info("=== watch_realtime start ===")

    today = _today_str()

    # load existing data from zstore (all codes under this partkey for today)
    realtime_data = _load_from_zstore(today, None)
    if realtime_data:
        logging.info("Loaded %d codes from zstore", len(realtime_data))

    # per-code aggregator state (shared, protected by data_lock)
    state = {}

    data_queue = queue.Queue(maxsize=100)
    stop_event = threading.Event()
    data_lock = threading.Lock()

    # shared mutable code list (protected by data_lock)
    watch_ctx = {
        "file": watchlist_file,
        "mtime": 0,
        "codes": [],
    }

    # initial load (treats as first reload from empty state)
    _reload_watchlist_if_changed(watch_ctx, realtime_data, state, data_lock)

    if not watch_ctx["codes"]:
        print(f"No codes found in {watchlist_file}", file=sys.stderr)
        return 1

    logging.info("Watching %d codes", len(watch_ctx["codes"]))

    def _handle_sigterm(signum, frame):
        logging.info("Received signal %d, shutting down", signum)
        stop_event.set()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    fetcher = threading.Thread(
        target=fetcher_thread, args=(data_queue, stop_event, today, watch_ctx, realtime_data, state, data_lock),
        name="fetcher", daemon=True)
    aggregator = threading.Thread(
        target=aggregator_thread, args=(data_queue, realtime_data, state, data_lock, stop_event, today),
        name="aggregator", daemon=True)
    storer = threading.Thread(
        target=store_thread, args=(realtime_data, data_lock, stop_event, today),
        name="store", daemon=True)
    fetcher.start()
    aggregator.start()
    storer.start()

    try:
        while fetcher.is_alive() and aggregator.is_alive() and storer.is_alive():
            time.sleep(30)
            with data_lock:
                n = len(watch_ctx["codes"])
                cells = sum(len(realtime_data.get(c, [])) for c in watch_ctx["codes"]) // n if n else 0
                logging.info("Status: %d codes, ~%d cells each", n, cells)
                if watch_ctx["codes"]:
                    first = sorted(watch_ctx["codes"])[0]
                    b = realtime_data.get(first, [])
                    logging.info("  %s: %d cells, last price=%.4f",
                                 first, len(b), b[-1][0] if b else 0)
    finally:
        stop_event.set()
        fetcher.join(timeout=30)
        aggregator.join(timeout=30)
        storer.join(timeout=30)

    logging.info("Done.")
    return 0


# ----------------------------------------------------------------------
# fetcher

def fetcher_thread(data_queue, stop_event, today, watch_ctx, realtime_data, state, data_lock):
    """
    Thread 1: fetch raw Tencent realtime data every FETCH_INTERVAL seconds,
    aligned to FETCH_ALIGN_SEC second boundaries.
    Puts (fetch_time, raw_text) into queue; parsing is done in aggregator thread.
    Reloads watchlist file when it changes (detected before each fetch).
    """
    logging.info("Fetcher thread started (interval=%ds, align=%ds)",
                 FETCH_INTERVAL, FETCH_ALIGN_SEC)

    while not stop_event.is_set():
        now = time.time()
        now_cst_sec = (now % 86400 + 8 * 3600) % 86400

        # date changed — market day is over, exit
        if _today_str() != today:
            logging.info("Date changed, fetcher exiting")
            break

        # -- schedule control --
        if now_cst_sec < MORNING_START_SEC - 60:
            if stop_event.wait(min(MORNING_START_SEC - 60 - now_cst_sec, 3600)):
                break
            continue
        if MORNING_END_SEC + 300 <= now_cst_sec < AFTERNOON_START_SEC - 60:
            if stop_event.wait(min(AFTERNOON_START_SEC - 60 - now_cst_sec, 7200)):
                break
            continue
        if now_cst_sec > AFTERNOON_END_SEC + 300:
            logging.info("Market closed + 5min, fetcher exiting")
            break

        # -- check watchlist file for changes --
        _reload_watchlist_if_changed(watch_ctx, realtime_data, state, data_lock)

        # -- align to next fetch boundary --
        offset = (FETCH_ALIGN_SEC - int(now) % FETCH_INTERVAL) % FETCH_INTERVAL
        if offset == 0:
            offset = FETCH_INTERVAL
        next_time = int(now) + offset
        while not stop_event.is_set():
            remain = next_time - time.time()
            if remain <= 0.05 or stop_event.wait(remain):
                break
        if stop_event.is_set():
            break

        # -- fetch raw data --
        with data_lock:
            codes = list(watch_ctx["codes"])
        if not codes:
            continue
        url = f"https://qt.gtimg.cn/q={','.join(codes)}&r={int(time.time() * 1000)}"
        raw = None
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            with urlopen(req, timeout=8) as resp:
                data = resp.read()
                for enc in ("gb18030", "utf8"):
                    try:
                        raw = data.decode(enc)
                        break
                    except UnicodeDecodeError:
                        continue
                if raw is None:
                    raw = data.decode("gb18030", errors="replace")
        except Exception as e:
            logging.warning("fetch fail: %s", e)

        if raw:
            try:
                data_queue.put_nowait((time.time(), raw))
            except queue.Full:
                logging.warning("Data queue full, dropping frame")
        else:
            logging.warning("Fetch returned empty")


# ----------------------------------------------------------------------
# aggregator

def aggregator_thread(data_queue, realtime_data, state, data_lock, stop_event, today):
    """
    Thread 2: parse raw Tencent data and build 1-min bars for each code.
    realtime_data[code] = [[price, outer, inner], ...] (protected by data_lock).
    state[code] = {last_trade, last_outer, last_inner} (protected by data_lock).
    New codes are added to state dynamically by the watchlist reload mechanism.
    """
    logging.info("Aggregator thread started")

    while not stop_event.is_set():
        try:
            fetch_ts, raw_text = data_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        # parse raw response: {code: {ts, price, outer_lots, inner_lots, amount_wan}}
        raw_data = _parse_tencent(raw_text)
        if not raw_data:
            continue

        # validate + convert to trade seconds; drop stale/wrong-date data
        data = {}
        for code, item in raw_data.items():
            if not _is_ts_valid_today(item["ts"], today):
                continue
            ts_trade = _trade_sec(_ts_sec(item["ts"]))
            if ts_trade is None:
                continue
            data[code] = (ts_trade, item["price"], item["outer_lots"], item["inner_lots"])
        if not data:
            continue

        with data_lock:
            for code, (ts_trade, price, outer, inner) in data.items():
                # skip codes not currently watched (e.g. removed from watchlist)
                if code not in state:
                    continue
                update_one(realtime_data, state, code, ts_trade, price, outer, inner)

            # write to memcache after each batch
            _write_memcache(today, realtime_data)


# ----------------------------------------------------------------------
# store

def store_thread(realtime_data, data_lock, stop_event, today):
    """
    Thread 3: periodically upsert realtime_data to zstore during market hours.
    Wakes every STORE_INTERVAL seconds; also writes once on exit.
    """
    if not ZSTORE_URL or not ZSTORE_AUTHKEY:
        logging.warning("ZSTORE_URL/ZSTORE_AUTHKEY not set, store thread disabled")
        return

    try:
        z = zstore(ZSTORE_URL, backend="cosmos", authkey=ZSTORE_AUTHKEY)
    except Exception as e:
        logging.warning("zstore init failed: %s", e)
        return

    logging.info("Store thread started (interval=%ds)", STORE_INTERVAL)

    while not stop_event.is_set():
        _store_to_zstore(z, realtime_data, data_lock, today)
        # sleep in small chunks so stop_event responds quickly
        for _ in range(STORE_INTERVAL):
            if stop_event.is_set():
                break
            time.sleep(1)

    # final store on exit
    _store_to_zstore(z, realtime_data, data_lock, today)
    logging.info("Store thread exiting")


# update realtime_data of code with newly fetched realtime data
#
# Walk forward from last_trade to curr_trade: at each cell boundary crossed,
# interpolate cumulative volume and += the increment to that cell.
# Remaining increment after the last boundary goes to the current (live) cell.
# Caller is responsible for validation and ts_trade conversion.
def update_one(realtime_data, state, code, ts_trade, price, outer, inner):
    if code not in state:
        return
    st = state[code]
    bars = realtime_data[code]

    if ts_trade < st["last_trade"]:
        return  # older timestamp, skip (equal timestamps update price)

    curr_cell = ts_trade // CELL_SECONDS
    if curr_cell >= TOTAL_CELLS:
        curr_cell = TOTAL_CELLS - 1

    prev_trade = st["last_trade"]
    prev_outer = st["last_outer"]
    prev_inner = st["last_inner"]

    # walk through cell boundaries between prev and current (in trade seconds)
    cell_idx = prev_trade // CELL_SECONDS
    if cell_idx >= TOTAL_CELLS:
        cell_idx = TOTAL_CELLS - 1

    while cell_idx <= curr_cell and cell_idx < TOTAL_CELLS:
        boundary_trade = (cell_idx + 1) * CELL_SECONDS
        if boundary_trade <= prev_trade:
            cell_idx += 1
            continue
        if boundary_trade > ts_trade:
            break  # current data point is still inside this cell

        # interpolate cumulative volume at boundary (in trade-seconds space)
        outer_at_b = _interp(prev_outer, outer, prev_trade, ts_trade, boundary_trade)
        inner_at_b = _interp(prev_inner, inner, prev_trade, ts_trade, boundary_trade)

        inc_outer = outer_at_b - prev_outer
        inc_inner = inner_at_b - prev_inner
        if inc_outer < 0 or inc_inner < 0:
            break  # cumulative volume should not decrease; stop and don't update state

        while len(bars) <= cell_idx:
            bars.append([0.0, 0.0, 0.0])
        bars[cell_idx][0] = round(price, 4)
        bars[cell_idx][1] += round(inc_outer, 2)
        bars[cell_idx][2] += round(inc_inner, 2)

        prev_outer = outer_at_b
        prev_inner = inner_at_b
        prev_trade = boundary_trade
        cell_idx += 1

    # remaining increment goes to the current (live) cell
    inc_outer = outer - prev_outer
    inc_inner = inner - prev_inner

    while len(bars) <= curr_cell:
        bars.append([0.0, 0.0, 0.0])
    bars[curr_cell][0] = round(price, 4)

    if inc_outer >= 0 and inc_inner >= 0:
        bars[curr_cell][1] += round(inc_outer, 2)
        bars[curr_cell][2] += round(inc_inner, 2)
        st["last_outer"] = outer
        st["last_inner"] = inner
        st["last_trade"] = ts_trade


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------

def _load_from_zstore(today, codes):
    """ load today's bars from zstore, returns {code: [[price, outer, inner], ...]} """
    if not ZSTORE_URL or not ZSTORE_AUTHKEY:
        logging.warning("zstore init no config")
        return {}
    try:
        z = zstore(ZSTORE_URL, backend="cosmos", authkey=ZSTORE_AUTHKEY)
    except Exception as e:
        logging.warning("zstore init failed: %s", e)
        return {}
    try:
        rows = z.select(ZSTORE_TABLE, fields=["id", "code", "bars"],
                where={"and": [["partkey_", "=", ZSTORE_PARTKEY], ["date", "=", int(today)]]})
    except Exception as e:
        logging.warning("load from zstore failed: %s", e)
        return {}
    result = {}
    for row in rows:
        code = row.get("code", "")
        if codes and code not in codes:
            continue
        bars_str = row.get("bars", "[]")
        try:
            bars = json.loads(bars_str) if isinstance(bars_str, str) else bars_str
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(bars, list):
            result[code] = bars
    return result


def _store_to_zstore(z, realtime_data, data_lock, today):
    """ upsert all non-empty bars to zstore """
    with data_lock:
        rows = []
        for code, bars in realtime_data.items():
            if not bars:
                continue
            rows.append({
                "partkey_": ZSTORE_PARTKEY,
                "id": f"{today}.{code}",
                "date": int(today),
                "code": code,
                "bars": json.dumps(bars),
            })
    if not rows:
        return
    try:
        res = z.upsert(ZSTORE_TABLE, rows)
        if isinstance(res, dict) and "succeeded" in res:
            logging.info("Stored %d/%d rows", res["succeeded"], res["total"])
        else:
            logging.info("Stored %d rows", len(rows))
    except Exception as e:
        logging.warning("zstore upsert failed: %s", e)


# shared memcache client (lazily created)
_memcache_client = None

def _write_memcache(today, realtime_data):
    """ write latest bars to memcached, one key per code; call must hold data_lock """
    global _memcache_client
    if not MEMCACHE_ADDR:
        return
    if _memcache_client is None:
        try:
            host, port = MEMCACHE_ADDR.rsplit(":", 1)
            _memcache_client = _pymemcache_base.Client((host, int(port)))
        except Exception as e:
            logging.warning("memcache init failed: %s", e)
            return
    try:
        for code, bars in realtime_data.items():
            key = MEMCACHE_KEY_PREFIX + code
            value = json.dumps({today: {code: bars}}).encode("utf-8")
            _memcache_client.set(key, value)
    except Exception as e:
        logging.warning("memcache write failed: %s", e)


def _load_codes_file(filepath):
    """ load code list from jsonl file, returns list of code strings """
    codes = []
    seen = set()
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                code = obj.get("code", "").strip()
                if code and code not in seen:
                    seen.add(code)
                    codes.append(code)
            except (json.JSONDecodeError, AttributeError):
                continue
    return codes


def _reload_watchlist_if_changed(watch_ctx, realtime_data, state, data_lock):
    """
    Check if watchlist file has been modified. If so, reload and update
    the shared code list, realtime_data, and state. New codes get initialized;
    removed codes stay in realtime_data (for zstore) but are removed from state
    (stop aggregating). Initial load (mtime == 0) is treated the same way.
    """
    filepath = watch_ctx["file"]
    try:
        new_mtime = os.path.getmtime(filepath)
    except OSError:
        return
    if new_mtime == watch_ctx["mtime"]:
        return

    new_codes = _load_codes_file(filepath)
    if not new_codes:
        logging.warning("Watchlist reload: file empty, keeping old list")
        return

    with data_lock:
        old_codes = set(watch_ctx["codes"])
        new_set = set(new_codes)
        added = new_set - old_codes
        removed = old_codes - new_set

        # initialize state + bars for newly added codes.
        # new codes with no prior bars start from 0; the first data point's
        # cumulative volume is interpolated across all prior cells (flat spread).
        for code in added:
            if code not in realtime_data:
                realtime_data[code] = []
            state[code] = _build_state_from_bars(realtime_data[code])

        # stop tracking removed codes
        for code in removed:
            state.pop(code, None)

        watch_ctx["codes"] = new_codes
        watch_ctx["mtime"] = new_mtime

    logging.info("Watchlist reloaded: %d codes (+%d, -%d)",
                 len(new_codes), len(added), len(removed))


def _build_state_from_bars(bars):
    """
    Reconstruct aggregator state from existing bars.
    Assumes all existing bars are full (finalized).
    last_trade = last second of the last bar (inclusive).
    last_outer/inner = sum of all bars = cumulative volume at last_trade.
    """
    n = len(bars)
    sum_outer = sum(b[1] for b in bars) if n > 0 else 0.0
    sum_inner = sum(b[2] for b in bars) if n > 0 else 0.0
    last_trade = n * CELL_SECONDS - 1 if n > 0 else 0

    return {
        "last_trade": last_trade,
        "last_outer": sum_outer,
        "last_inner": sum_inner,
    }


def _today_str():
    """ return YYYYMMDD of current date in GMT+8 """
    t = time.gmtime(int(time.time()) + 8 * 3600)
    return f"{t.tm_year}{t.tm_mon:02d}{t.tm_mday:02d}"


def _ts_sec(ts_str):
    """ YYYYMMDDHHMMSS -> seconds since 00:00 """
    return int(ts_str[8:10]) * 3600 + int(ts_str[10:12]) * 60 + int(ts_str[12:14])


def _is_ts_valid_today(ts_str, today):
    """
    Check if timestamp belongs to today's allowed range
    (morning start - 60s ~ afternoon end + 300s).
    Returns False for wrong date or out-of-range time.
    """
    if len(ts_str) < 14 or ts_str[:8] != today:
        return False
    sec = _ts_sec(ts_str)
    return MORNING_START_SEC - 60 <= sec <= AFTERNOON_END_SEC + 300


def _trade_sec(t_sec):
    """
    Wall-clock seconds -> continuous trading seconds.
    Morning [9:30, 11:30)   -> 0..7199
    Afternoon [13:00, 15:00) -> 7200..14399
    After-close (up to +5min) clamps to the last second of each session
    (so post-close volume all goes into the last bar).
    Returns None if outside trading hours + grace period.
    """
    if MORNING_START_SEC <= t_sec < MORNING_END_SEC:
        return t_sec - MORNING_START_SEC
    if MORNING_END_SEC <= t_sec <= MORNING_END_SEC + 300:
        return CELLS_PER_SESSION * CELL_SECONDS - 1  # last second of morning
    if AFTERNOON_START_SEC <= t_sec < AFTERNOON_END_SEC:
        return CELLS_PER_SESSION * CELL_SECONDS + (t_sec - AFTERNOON_START_SEC)
    if AFTERNOON_END_SEC <= t_sec <= AFTERNOON_END_SEC + 300:
        return TOTAL_CELLS * CELL_SECONDS - 1  # last second of day
    return None


def _interp(prev_val, curr_val, prev_sec, curr_sec, boundary_sec):
    """ linear interpolation of cumulative value at boundary """
    dt = curr_sec - prev_sec
    if dt <= 0:
        return curr_val
    ratio = (boundary_sec - prev_sec) / dt
    return prev_val + (curr_val - prev_val) * ratio


def _parse_tencent(text):
    """ parse Tencent raw response into {code: {ts, price, outer_lots, inner_lots, amount_wan}} """
    if not text:
        return {}
    result = {}
    for m in re.finditer(r'v_(\w+)="([^"]*)"', text):
        tcode = m.group(1)
        a = m.group(2).split("~")
        if len(a) < 40:
            continue
        ts = a[30] if len(a) > 30 else ""
        if len(ts) < 14 or not tcode:
            continue
        try:
            result[tcode] = {
                "ts": ts,
                "price": float(a[3]),
                "outer_lots": float(a[7]),
                "inner_lots": float(a[8]),
                "amount_wan": float(a[37]),
            }
        except (ValueError, IndexError):
            continue
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv))
