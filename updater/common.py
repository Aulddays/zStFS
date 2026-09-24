import tqdm as _tqdm
import logging
import os
from contextlib import contextmanager
from dotenv import load_dotenv
import httpx
import datetime
import math

zstfsurl = None

def setup():
    # setup logs
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%Y%m%d-%H%M%S", level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.addLevelName(logging.CRITICAL, "CRI")
    logging.addLevelName(logging.ERROR, "ERR")
    logging.addLevelName(logging.WARNING, "WRN")
    logging.addLevelName(logging.INFO, "INF")
    logging.addLevelName(logging.DEBUG, "DBG")
    # add custom levels
    if not hasattr(logging, "VERBOSE"):
        logging.VERBOSE = logging.DEBUG + (logging.INFO - logging.DEBUG) // 3
        logging.addLevelName(logging.VERBOSE, "VRB")
        def logging_verbose(msg, *args, **kwargs):
            logging.log(logging.VERBOSE, msg, *args, **kwargs)
        logging.verbose = logging_verbose
        def logger_verbose(self, msg, *args, **kwargs):
            if self.isEnabledFor(logging.VERBOSE):
                self._log(logging.VERBOSE, msg, args, **kwargs)
        logging.Logger.verbose = logger_verbose
    if not hasattr(logging, "TRACE"):
        logging.TRACE = logging.DEBUG + (logging.INFO - logging.DEBUG) // 3 * 2
        logging.addLevelName(logging.TRACE, "TRC")
        def logging_trace(msg, *args, **kwargs):
            logging.log(logging.TRACE, msg, *args, **kwargs)
        logging.trace = logging_trace
        def logger_trace(self, msg, *args, **kwargs):
            if self.isEnabledFor(logging.TRACE):
                self._log(logging.TRACE, msg, args, **kwargs)
        logging.Logger.trace = logger_trace
    logging.getLogger().setLevel(logging.VERBOSE)

    # setup env
    load_dotenv()
    global zstfsurl
    zstfsurl = os.getenv("ZSTFS_URL", None)
    if zstfsurl is None:
        raise ValueError("ZSTFS_URL not found")
    

@contextmanager
def _quiet_tqdm():
    """
    Context manager that silences tqdm progress bars inside the with-block.
    Temporarily forces disable=True on all new tqdm instances; restores
    the original __init__ on exit, so user code outside the block is unaffected.
    """
    orig_init = _tqdm.tqdm.__init__

    def _disabled_init(self, *args, **kwargs):
        kwargs["disable"] = True
        orig_init(self, *args, **kwargs)

    _tqdm.tqdm.__init__ = _disabled_init
    try:
        yield
    finally:
        _tqdm.tqdm.__init__ = orig_init

def safe_call(func, *args, **kwargs):
    """Call an akshare function safely; return None on failure."""
    try:
        with _quiet_tqdm():
            return func(*args, **kwargs)
    except Exception as e:
        logging.warning(f"{func.__name__} failed: {e}")
        return None


def cna_code(code, prefix=None):
    """
    normalize CNA symbol code
    Input: code: 6 digit code with or without prefix. prefix: None/sh/sz/bj/of
        If input `code` has prefix, use it; otherwise use `prefix`; otherwise determin based on code range
    Output: code (with prefix sh/sz/bj/of), exchange (ZH/SZ/BJ/""), board (Main/STAR/ChiNext/BJ/"")
    """
    code = str(code).strip().lower()
    # strip prefix in code, if present
    pfx = ""
    if len(code) >= 2 and code[:2] in {"sh", "sz", "bj", "of"}:
        pfx, code = code[:2], code[2:]
    if not (len(code) == 6 and code.isdigit()):
        raise ValueError(f"invalid code: {pfx}{code}")
    # resolve prefix: code > param > inferred
    if pfx == "":
        pfx = prefix
    if pfx is None:
        if code.startswith("6"):
            pfx = "sh"
        elif code.startswith("0") or code.startswith("3"):
            pfx = "sz"
        elif code[0] in "489":
            pfx = "bj"
        else:
            raise ValueError(f"cannot infer prefix for code {code}")
    # exchange and board
    if pfx == "sh":
        exchange, board = "SH", "STAR" if code[:3] in {"688", "689"} else "Main"
    elif pfx == "sz":
        exchange, board = "SZ", "ChiNext" if code[:3] in {"300", "301"} else "Main"
    elif pfx == "bj":
        exchange, board = "BJ", "BJ"
    else:
        exchange = board = ""

    return pfx + code, exchange, board
    

def cna_fixname(name):
    return name.replace(" ", "")


def put_symbols(records, market):
    if not records:
        logging.warning("No records to put")
        return
    url = f"{zstfsurl}/v1/markets/{market}/symbols"
    # Build the payload: list of symbol dicts matching the server's ParseSymbol schema.
    payload = records
    logging.verbose("Writing %d symbols %s ...", len(payload), url)
    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        logging.verbose("Upload done: accepted=%d changed=%d unchanged=%d",
                     data.get("accepted", 0),
                     data.get("changed", 0),
                     data.get("unchanged", 0))
    except httpx.HTTPError as e:
        logging.error("Put failed: %s", e)
        raise


def get_bar(market, code, frequency, time=None):
    url = f"{zstfsurl}/v1/markets/{market}/bars?code={code}&frequency={frequency}"
    if isinstance(time, str) and time != "":
        url += f"&time={time}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
        return data.get("bar")
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None
    except Exception as e:
        logging.error("Get failed: %s", e)
        raise


def get_bars(market, code, frequency, start_date, end_date):
    url = f"{zstfsurl}/v1/markets/{market}/bars?code={code}&frequency={frequency}&begin={start_date}&end={end_date}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
        return data.get("bars")
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None
    except Exception as e:
        logging.error("Get failed: %s", e)
        raise


def put_bars(records, market):
    if not records:
        logging.warning("No records to put")
        return
    url = f"{zstfsurl}/v1/markets/{market}/bars"
    # Build the payload: list of bars dicts.
    payload = records
    logging.verbose("Writing %d bars %s ...", len(payload), url)
    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        logging.verbose("%s", data)
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def fixfloat(val):
    """Safely convert value to float; return None on failure or NaN."""
    if val is None or val == "":
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None

def fixint(val):
    if isinstance(val, str):
        val = val.replace(",", "")
    return int(val)
