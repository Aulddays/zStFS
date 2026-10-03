"""
Python client for zStFS HTTP daemon (zstfsd).
"""

import logging
import os
from dotenv import load_dotenv
import httpx


zstfsurl = None


def setup():
    # setup env
    load_dotenv()
    global zstfsurl
    zstfsurl = os.getenv("ZSTFS_URL", None)
    if zstfsurl is None:
        raise ValueError("ZSTFS_URL not found")


# ----------------------------------------------------------------------
# health
# ----------------------------------------------------------------------

def health():
    """GET /v1/health — daemon liveness check."""
    url = f"{zstfsurl}/v1/health"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


# ----------------------------------------------------------------------
# symbols
# ----------------------------------------------------------------------

def list_symbols(market):
    """
    GET /v1/markets/<market>/symbols
    List all symbols with full metadata and alias lists.
    """
    url = f"{zstfsurl}/v1/markets/{market}/symbols"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def get_symbol(market, code):
    """
    GET /v1/markets/<market>/symbols/<code>
    Fetch a single symbol by code.  Returns None on 404.
    """
    url = f"{zstfsurl}/v1/markets/{market}/symbols/{code}"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def put_symbols(market, records):
    """
    POST /v1/markets/<market>/symbols — upsert one or more symbols.

    `records` may be a single symbol dict, a list of dicts, or
    {"symbols": [...]}.
    """
    if not records:
        logging.warning("No records to put")
        return
    url = f"{zstfsurl}/v1/markets/{market}/symbols"
    logging.verbose("Writing %d symbols %s ...",
                    len(records) if isinstance(records, list) else 1, url)
    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(url, json=records)
            resp.raise_for_status()
            data = resp.json()
        logging.verbose("Upload done: %s", data)
        return data
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def delete_symbol(market, code):
    """
    DELETE /v1/markets/<market>/symbols/<code>
    Soft-delete (retire) a symbol by code.
    """
    url = f"{zstfsurl}/v1/markets/{market}/symbols/{code}"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.delete(url)
            resp.raise_for_status()
            return resp.json() if resp.content else None
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


# ----------------------------------------------------------------------
# bars
# ----------------------------------------------------------------------

def get_bar(market, code=None, symbol_id=None, frequency="daily", time=None):
    """
    GET /v1/markets/<market>/bars — single bar lookup.

    One of `code` or `symbol_id` must be provided.
    If `time` is omitted the latest bar is returned.
    Returns the bar dict, or None if not found (404).
    """
    url = f"{zstfsurl}/v1/markets/{market}/bars"
    params = _bar_query_params(code, symbol_id, frequency)
    if time:
        params["time"] = time
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url, params=params)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
        return data.get("bar")
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def get_bars(market, code=None, symbol_id=None, frequency="daily",
             begin=None, end=None, adjust=None):
    """
    GET /v1/markets/<market>/bars — range query.

    One of `code` or `symbol_id` must be provided.
    `adjust`: "raw" (default), "forward", or "backward".
    Returns a list of bar dicts (empty on 404).
    """
    url = f"{zstfsurl}/v1/markets/{market}/bars"
    params = _bar_query_params(code, symbol_id, frequency)
    if begin is not None:
        params["begin"] = begin
    if end is not None:
        params["end"] = end
    if adjust is not None:
        params["adjust"] = adjust
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(url, params=params)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
        return data.get("bars", [])
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def put_bars(market, records):
    """
    POST /v1/markets/<market>/bars — write one or more bars.

    `records` may be a single bar dict, a list, or {"bars":[...]}.
    Each bar identifies the symbol via `symbol_id` or `code`,
    and includes `frequency`, `time`, `state`, and OHLCV fields.
    """
    if not records:
        logging.warning("No records to put")
        return
    url = f"{zstfsurl}/v1/markets/{market}/bars"
    logging.verbose("Writing %d bars %s ...",
                    len(records) if isinstance(records, list) else 1, url)
    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(url, json=records)
            resp.raise_for_status()
            data = resp.json()
        logging.verbose("%s", data)
        return data
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def _bar_query_params(code, symbol_id, frequency):
    params = {"frequency": frequency}
    if code is not None:
        params["code"] = code
    elif symbol_id is not None:
        params["symbol_id"] = symbol_id
    else:
        raise ValueError("either code or symbol_id must be provided")
    return params


# ----------------------------------------------------------------------
# actions
# ----------------------------------------------------------------------

def put_action(market, action):
    """
    POST /v1/markets/<market>/actions — upsert a corporate action.

    `action` is a dict with: external_event_key, symbol_id or code,
    effective_date, type, factor, cash_value.
    """
    url = f"{zstfsurl}/v1/markets/{market}/actions"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json=action)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def get_actions(market, code=None, symbol_id=None, begin=None, end=None):
    """
    GET /v1/markets/<market>/actions — query corporate actions.

    One of `code` or `symbol_id` must be provided.
    `begin` / `end` are optional YYYYMMDD strings.
    """
    url = f"{zstfsurl}/v1/markets/{market}/actions"
    params = {}
    if code is not None:
        params["code"] = code
    elif symbol_id is not None:
        params["symbol_id"] = symbol_id
    else:
        raise ValueError("either code or symbol_id must be provided")
    if begin is not None:
        params["begin"] = begin
    if end is not None:
        params["end"] = end
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


def delete_action(market, external_event_key, code=None, symbol_id=None):
    """
    DELETE /v1/markets/<market>/actions — idempotent action removal.

    One of `code` or `symbol_id` must be provided together with
    `external_event_key`.
    """
    url = f"{zstfsurl}/v1/markets/{market}/actions"
    params = {"external_event_key": external_event_key}
    if code is not None:
        params["code"] = code
    elif symbol_id is not None:
        params["symbol_id"] = symbol_id
    else:
        raise ValueError("either code or symbol_id must be provided")
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.delete(url, params=params)
            resp.raise_for_status()
            return resp.json() if resp.content else None
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None


# ----------------------------------------------------------------------
# staging
# ----------------------------------------------------------------------

def stage():
    """
    GET /v1/stage — trigger Active → Staging seal for all markets.

    Returns {"trading_days_back": N}.
    """
    url = f"{zstfsurl}/v1/stage"
    logging.info("Initiating StagingStore seal ...")
    try:
        with httpx.Client(timeout=240) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
        logging.info("%s", data)
        return data
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code} {e.response.reason_phrase}: {e.response.text}"
        logging.error("%s", msg)
        raise ConnectionError(msg) from None
