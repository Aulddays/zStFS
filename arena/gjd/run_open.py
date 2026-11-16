#!/usr/bin/env python3
"""
Intraday data aggregator for GJD ETFs.

Fetch data from Tencent API every 10 sec (aligned to :05/:15/...).
Aggregate into 0.5-min (30s) bars:
    `bars[code] = [[price, outer_lots, inner_lots], ...]`
"""

import sys
import os
import re
import json
import time
import logging
import threading
import queue
from urllib.request import urlopen, Request

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_this_dir))   # arena/
sys.path.insert(0, _this_dir)                      # arena/gjd/
import cna_calendar
from run_daily import ETF_LIST, setup_logging

# ----------------------------------------------------------------------
# configs
# ----------------------------------------------------------------------

FETCH_INTERVAL = 10       # seconds between fetches
FETCH_ALIGN_SEC = 5       # align to :05/:15/:25/... second boundaries

MORNING_START_SEC = 9 * 3600 + 30 * 60     # 09:30:00
MORNING_END_SEC = 11 * 3600 + 30 * 60     # 11:30:00
AFTERNOON_START_SEC = 13 * 3600           # 13:00:00
AFTERNOON_END_SEC = 15 * 3600             # 15:00:00

CELL_SECONDS = 30         # 0.5 min per cell
CELLS_PER_SESSION = 240   # 120 min / 0.5 min
TOTAL_CELLS = CELLS_PER_SESSION * 2  # 480 cells per day


def main(argv):
    setup_logging()

    if not cna_calendar.isopen():
        logging.info("Today is not a trading day, exit.")
        return 0

    logging.info("=== run_open start ===")

    today = _today_str()
    detail_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               f"{today}.detail.json")

    # load existing data if any
    gjd_realtime_data = _load_detail(detail_path)  # {code: [[price, outer, inner], ...]}
    if gjd_realtime_data:
        n = len(gjd_realtime_data)
        cells = sum(len(v) for v in gjd_realtime_data.values()) // n
        logging.info("Loaded %d ETFs from %s (%d cells each)", n, detail_path, cells)

    data_queue = queue.Queue(maxsize=100)
    stop_event = threading.Event()
    data_lock = threading.Lock()

    fetcher = threading.Thread(
        target=fetcher_thread, args=(data_queue, stop_event),
        name="fetcher", daemon=True)
    aggregator = threading.Thread(
        target=aggregator_thread, args=(data_queue, gjd_realtime_data, data_lock, stop_event),
        name="aggregator", daemon=True)
    fetcher.start()
    aggregator.start()

    try:
        while fetcher.is_alive() and aggregator.is_alive():
            time.sleep(30)
            with data_lock:
                n = len(gjd_realtime_data)
                cells = sum(len(v) for v in gjd_realtime_data.values()) // n if n else 0
                logging.info("Status: %d ETFs, ~%d cells each", n, cells)
                if gjd_realtime_data:
                    first = sorted(gjd_realtime_data.keys())[0]
                    b = gjd_realtime_data[first]
                    logging.info("  %s: %d cells, last price=%.4f",
                                 first, len(b), b[-1][0] if b else 0)
    except KeyboardInterrupt:
        logging.info("Interrupted")
    finally:
        stop_event.set()
        fetcher.join(timeout=5)
        aggregator.join(timeout=5)
        with data_lock:
            _save_detail(detail_path, gjd_realtime_data)
        logging.info("Saved detail to %s", detail_path)

    logging.info("Done.")
    return 0


# ----------------------------------------------------------------------
# fetcher

def fetcher_thread(data_queue, stop_event):
    """
    Thread 1: fetch raw Tencent realtime data every FETCH_INTERVAL seconds,
    aligned to FETCH_ALIGN_SEC second boundaries.
    Puts (fetch_time, raw_text) into queue; parsing is done in aggregator thread.
    """
    tcodes = [e["tcode"] for e in ETF_LIST]
    logging.info("Fetcher thread started (%d ETFs, interval=%ds, align=%ds)",
                 len(tcodes), FETCH_INTERVAL, FETCH_ALIGN_SEC)

    while not stop_event.is_set():
        now = time.time()
        now_cst_sec = (now % 86400 + 8 * 3600) % 86400

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
        url = f"https://qt.gtimg.cn/q={','.join(tcodes)}&r={int(time.time() * 1000)}"
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

def aggregator_thread(data_queue, gjd_realtime_data, data_lock, stop_event):
    """
    Thread 2: parse raw Tencent data and build 0.5-min bars for each ETF.
    gjd_realtime_data[code] = [[price, outer, inner], ...] (protected by data_lock).
    """
    # per-ETF state
    state = {}
    for etf in ETF_LIST:
        code = etf["code"]
        bars = gjd_realtime_data.get(code, [])
        if code not in gjd_realtime_data:
            gjd_realtime_data[code] = []
        state[code] = _build_state_from_bars(bars)

    logging.info("Aggregator thread started (%d ETFs)", len(state))

    while not stop_event.is_set():
        try:
            fetch_ts, raw_text = data_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        # parse raw response: {code: {ts, price, outer_lots, inner_lots, amount_wan}}
        data = _parse_tencent(raw_text)
        if not data:
            continue

        with data_lock:
            for code, item in data.items():
                update_one(gjd_realtime_data, state, code, item)


# update gjd_realtime_data of code with newly fetched realtime data
def update_one(gjd_realtime_data, state, code, data):
    if code not in state:
        return
    st = state[code]
    bars = gjd_realtime_data[code]
    ts_str = data["ts"]

    if ts_str <= st["last_ts"]:
        return  # timestamp not advanced, skip

    ts_sec = _ts_sec(ts_str)    # YYYYMMDDHHMMSS -> seconds since 00:00

    # -- determine session --
    if MORNING_START_SEC <= ts_sec <= MORNING_END_SEC + 300:
        session = 0
    elif AFTERNOON_START_SEC <= ts_sec <= AFTERNOON_END_SEC + 300:
        session = 1
    else:
        return

    # -- session transition (morning -> afternoon): reset per-session state
    # outer/inner are daily cumulative; baseline afternoon at morning's last value
    # so that afternoon cells contain only incremental afternoon volume.
    if st["session"] != session and session == 1:
        st["prev_outer"] = st["last_outer"]
        st["prev_inner"] = st["last_inner"]
        st["last_finalized"] = None
        st["started"] = False
        st["session"] = 1
        st["last_ts"] = ""

    curr_cell = _cell_for_sec(ts_sec)

    # -- first data of a session --
    if not st["started"]:
        st["started"] = True
        st["session"] = session
        st["last_outer"] = data["outer_lots"]
        st["last_inner"] = data["inner_lots"]
        st["last_ts"] = ts_str

        if curr_cell is None:
            # already past close — finalize last cell directly
            last_idx = CELLS_PER_SESSION - 1 if session == 0 else TOTAL_CELLS - 1
            _finalize_last(st, bars, last_idx, data)
        else:
            while len(bars) <= curr_cell:
                bars.append([0.0, 0.0, 0.0])
            bars[curr_cell] = [
                round(data["price"], 4),
                round(max(0, data["outer_lots"] - st["prev_outer"]), 2),
                round(max(0, data["inner_lots"] - st["prev_inner"]), 2),
            ]
        return

    prev_sec = _ts_sec(st["last_ts"])
    prev_cell = _cell_for_sec(prev_sec)

    # -- data after close: finalize last cell --
    if curr_cell is None:
        last_idx = CELLS_PER_SESSION - 1 if session == 0 else TOTAL_CELLS - 1
        _finalize_last(st, bars, last_idx, data)
        st["last_outer"] = data["outer_lots"]
        st["last_inner"] = data["inner_lots"]
        st["last_ts"] = ts_str
        return

    assert prev_cell is not None, "prev_cell is None but started and in-session"

    # -- finalize all cells whose boundary has been crossed --
    start_idx = (st["last_finalized"] + 1) if st["last_finalized"] is not None else \
                (0 if session == 0 else CELLS_PER_SESSION)

    cell_idx = start_idx
    while cell_idx <= curr_cell and cell_idx < TOTAL_CELLS:
        boundary = _cell_end_sec(cell_idx)
        if boundary <= prev_sec:
            cell_idx += 1
            continue
        if boundary > ts_sec:
            break

        # interpolate cumulative volume at boundary
        outer_at_b = _interp(st["last_outer"], data["outer_lots"],
                                prev_sec, ts_sec, boundary)
        inner_at_b = _interp(st["last_inner"], data["inner_lots"],
                                prev_sec, ts_sec, boundary)

        cell_outer = outer_at_b - st["prev_outer"]
        cell_inner = inner_at_b - st["prev_inner"]

        while len(bars) <= cell_idx:
            bars.append([0.0, 0.0, 0.0])
        bars[cell_idx] = [round(data["price"], 4),
                            round(cell_outer, 2),
                            round(cell_inner, 2)]

        st["prev_outer"] = outer_at_b
        st["prev_inner"] = inner_at_b
        st["last_finalized"] = cell_idx
        cell_idx += 1

    # -- update state and live cell (in-progress) --
    st["last_outer"] = data["outer_lots"]
    st["last_inner"] = data["inner_lots"]
    st["last_ts"] = ts_str

    while len(bars) <= curr_cell:
        bars.append([0.0, 0.0, 0.0])
    bars[curr_cell][0] = data["price"]
    bars[curr_cell][1] = round(max(0, data["outer_lots"] - st["prev_outer"]), 2)
    bars[curr_cell][2] = round(max(0, data["inner_lots"] - st["prev_inner"]), 2)


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------

def _today_str():
    """ return YYYYMMDD of current date in GMT+8 """
    now = int(time.time())
    sec = (now % 86400 + 8 * 3600) % 86400
    day = now // 86400 + (1 if sec < 8 * 3600 else 0)
    # epoch is 1970-01-01, but let's use time module for safety
    import time as _t
    t = _t.gmtime(now + 8 * 3600)
    return f"{t.tm_year}{t.tm_mon:02d}{t.tm_mday:02d}"


def _load_detail(path):
    """ load bars from json file, returns {} on failure """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.warning("load detail failed: %s", e)
        return {}


def _save_detail(path, data):
    """ save bars to json file """
    try:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception as e:
        logging.warning("save detail failed: %s", e)


def _build_state_from_bars(bars):
    """
    Reconstruct aggregator state from existing (assumed all-finalized) bars.
    All bars are treated as finalized; last_ts = end time of last bar.
    """
    n = len(bars)
    if n == 0:
        return {
            "last_ts": "",
            "last_outer": 0.0,
            "last_inner": 0.0,
            "prev_outer": 0.0,
            "prev_inner": 0.0,
            "last_finalized": None,
            "session": 0,
            "started": False,
        }

    last_idx = n - 1
    in_afternoon = last_idx >= CELLS_PER_SESSION

    # sum outer/inner up to last_idx to get cumulative at last boundary
    sum_outer = sum(b[1] for b in bars[:n])
    sum_inner = sum(b[2] for b in bars[:n])

    # afternoon baseline: morning cumulative before afternoon starts
    # we don't know it exactly from bars alone, but all cells are finalized
    # so prev_outer = total sum up to last boundary (including this cell)
    prev_outer = sum_outer
    prev_inner = sum_inner

    # last_ts = end time of last cell, formatted as YYYYMMDDHHMMSS
    end_sec = _cell_end_sec(last_idx)
    hh = end_sec // 3600
    mm = (end_sec % 3600) // 60
    ss = end_sec % 60
    # date placeholder — update_one only compares lexicographically;
    # we use a dummy date so that real data (with real date) compares greater
    last_ts = f"00000000{hh:02d}{mm:02d}{ss:02d}"

    return {
        "last_ts": last_ts,
        "last_outer": prev_outer,
        "last_inner": prev_inner,
        "prev_outer": prev_outer,
        "prev_inner": prev_inner,
        "last_finalized": last_idx,
        "session": 1 if in_afternoon else 0,
        "started": True,
    }


def _ts_sec(ts_str):
    """ YYYYMMDDHHMMSS -> seconds since 00:00 """
    return int(ts_str[8:10]) * 3600 + int(ts_str[10:12]) * 60 + int(ts_str[12:14])


def _cell_end_sec(cell_idx):
    """ cell index -> end time (seconds since 00:00) """
    if cell_idx < CELLS_PER_SESSION:
        return MORNING_START_SEC + (cell_idx + 1) * CELL_SECONDS
    else:
        return AFTERNOON_START_SEC + (cell_idx - CELLS_PER_SESSION + 1) * CELL_SECONDS


def _cell_for_sec(t_sec):
    """ seconds since midnight -> cell index, or None if outside trading hours """
    if MORNING_START_SEC <= t_sec < MORNING_END_SEC:
        return min((t_sec - MORNING_START_SEC) // CELL_SECONDS, CELLS_PER_SESSION - 1)
    elif AFTERNOON_START_SEC <= t_sec < AFTERNOON_END_SEC:
        return CELLS_PER_SESSION + min((t_sec - AFTERNOON_START_SEC) // CELL_SECONDS, CELLS_PER_SESSION - 1)
    return None


def _interp(prev_val, curr_val, prev_sec, curr_sec, boundary_sec):
    """ linear interpolation of cumulative value at boundary """
    dt = curr_sec - prev_sec
    if dt <= 0:
        return curr_val
    ratio = (boundary_sec - prev_sec) / dt
    return prev_val + (curr_val - prev_val) * ratio


def _finalize_last(st, bars, last_idx, item):
    """
    Finalize last cell of a session.
    All remaining volume belongs to it (no interpolation — boundary rule).
    """
    cell_outer = item["outer_lots"] - st["prev_outer"]
    cell_inner = item["inner_lots"] - st["prev_inner"]
    while len(bars) <= last_idx:
        bars.append([0.0, 0.0, 0.0])
    bars[last_idx] = [round(item["price"], 4),
                      round(cell_outer, 2),
                      round(cell_inner, 2)]
    st["last_finalized"] = last_idx
    st["prev_outer"] = item["outer_lots"]
    st["prev_inner"] = item["inner_lots"]


def _parse_tencent(text):
    """ parse Tencent raw response into {code: {ts, price, outer_lots, inner_lots, amount_wan}} """
    if not text:
        return {}
    result = {}
    for m in re.finditer(r'v_(\w+)="([^"]*)"', text):
        a = m.group(2).split("~")
        if len(a) < 40:
            continue
        code = a[2] if len(a) > 2 else ""
        ts = a[30] if len(a) > 30 else ""
        if len(ts) < 14 or not code:
            continue
        try:
            result[code] = {
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
