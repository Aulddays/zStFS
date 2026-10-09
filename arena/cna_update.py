import sys
import common
import akshare
import logging
import httpx
from datetime import datetime, timedelta
import time
from collections import defaultdict

tmstart = None

def main(argv):
    global tmstart
    tmstart = time.time()
    isschedule = len(argv) > 1 and argv[1] == "schedule"
    common.setup()
    cna_date(isschedule)
    
    stats = [None] * 4
    while True:
        idx = 0
        if stats[idx] is None or stats[idx][2] <= 0.995:
            symbols = cna_index_list()
            common.put_symbols(symbols, "cna")
            # print(records[0])
            # symbols = [{"code": "sh000001", 'list_date': '19910715'}]
            stats[idx] = cna_updatedata_daily(symbols)
            stats[idx].append(stats[idx][0] * 1.0 / stats[idx][1] if stats[idx][0] > 0 else 0)
            logging.info("Progress index: %s", stats[idx])

        idx += 1
        if stats[idx] is None or stats[idx][2] <= 0.995:
            symbols = cna_stock_list()
            common.put_symbols(symbols, "cna")
            # # print(symbols[:10])
            # symbols = [{'code': 'sh600004', 'name': '白云机场', 'list_date': '20030428'}]
            stats[idx] = cna_updatedata_daily(symbols)
            stats[idx].append(stats[idx][0] * 1.0 / stats[idx][1] if stats[idx][0] > 0 else 0)
            logging.info("Progress stock: %s", stats[idx])

        idx += 1
        if stats[idx] is None or stats[idx][2] <= 0.98:
            symbols = cna_etf_list()
            common.put_symbols(symbols, "cna")
            # print(symbols[0])
            stats[idx] = cna_updatedata_daily(symbols)
            stats[idx].append(stats[idx][0] * 1.0 / stats[idx][1] if stats[idx][0] > 0 else 0)
            logging.info("Progress etf: %s", stats[idx])

        idx += 1
        if stats[idx] is None or stats[idx][2] <= 0.95:
            symbols = cnof_list()
            common.put_symbols(symbols, "cnof")
            # print(symbols[:5])
            # symbols = [{'code': 'of000001', 'name': '华夏成长混合', 'values': {'20260924': 3.868, '20260923': 3.897}}]
            stats[idx] = cnof_updatedata(symbols)
            stats[idx].append(stats[idx][0] * 1.0 / stats[idx][1] if stats[idx][0] > 0 else 0)
            logging.info("Progress cnof: %s", stats[idx])

        if not isschedule:
            break
        logging.info("Progress: %s", stats)
        if (time.time() < tmstart or time.time() > tmstart + 8 * 3600 or
                all(s[2] > 0.95 for s in stats)):   # all finished
            break
        time.sleep(600)
    
    common.stage()

    return 0

# newest market open date for cna 
# wait_ready: If True, wait until there is newer data than local store, for at most 2 hours
_cna_date = None
_cna_open_dates = set()
def cna_date(wait_ready=False):
    global _cna_date
    while True:
        # get sh000001 data and get the newest date
        cur_date = datetime.now()
        if cur_date.strftime("%H%M") < "1530":
            cur_date -= timedelta(days=1)   # Today is not finished
        start_date = (cur_date - timedelta(days=31)).strftime("%Y%m%d")
        end_date = cur_date.strftime("%Y%m%d")
        logging.verbose("cna_date test range: %s %s", start_date, end_date)
        rawdata = common.safe_call(akshare.stock_zh_a_hist_tx,
                symbol="sh000001", start_date=start_date, end_date=end_date, adjust="")
        for __, row in rawdata.iterrows():
            # print(row)
            newdate = row["date"].strftime("%Y%m%d")
            _cna_open_dates.add(newdate)
            if _cna_date is None or newdate > _cna_date:
                _cna_date = newdate
        if _cna_date is None:
            _cna_date = end_date
        logging.info("CNA open date: %s", _cna_date)
        if _cna_date != end_date and wait_ready:
            # get local store date
            regdata = common.get_bar("cna", "sh000001", "daily")
            # print(regdata)
            if len(regdata) > 0:
                regdate = regdata["time"]
                logging.info("CNA store date: %s", regdate)
                if time.time() > tmstart and time.time() < tmstart + 7200 and regdate >= _cna_date:
                    logging.info("No new data. Wait 5 min...")
                    time.sleep(300)
                    continue
        break

def cna_index_list():
    logging.info("Fetching CNA Index list...")
    df = common.safe_call(akshare.stock_zh_index_spot_sina)
    if df is None:
        logging.warning("stock_zh_index_spot_sina failed")
        return []
    # Supplement with publish dates from index_stock_info
    df_info = common.safe_call(akshare.index_stock_info)
    pub_date_map = {}
    if df_info is not None:
        for _, row in df_info.iterrows():
            pub_date_map[str(row["index_code"])] = str(row.get("publish_date", ""))
    records = []
    for _, row in df.iterrows():
        code, exchange, __ = common.cna_code(str(row["代码"])) # Sina codes have sh/sz/bj prefix
        record = {
            "code": code,
            "name": common.cna_fixname(row["名称"]),
            "security_type": "index",
            "exchange": exchange,
            # "list_date": pub_date_map.get(code[2:], None),
        }
        list_date = pub_date_map.get(code[2:], None)
        if isinstance(list_date, str):
            list_date = list_date.replace("-", "")
            record["list_date"] = list_date
        # print(type(record["list_date"]))
        records.append(record)
    records.sort(key=lambda x: x["code"])
    logging.info("CNA Indeces: %d", len(records))
    return records


def cna_stock_list():
    logging.info("Fetching CNA stock list...")
    data = defaultdict(dict)
    # Get data from Tencent Finance real-time quotes
    # Tencent field mapping:
    # code(sh/sz/bj prefixed), name, zxj(最新价), zdf(涨跌幅), zd(涨跌), zf(振幅)
    # hsl(换手率), lb(量比), pe_ttm, zsz(总市值/100M), ltsz(流通市值/100M)
    # turnover(成交额), volume(成交量), speed?, pn(市净率), state?
    # zdf_d5/d10/d20/d60(5/10/20/60d 涨跌幅), zdf_y(YTD), zdf_w52(52w change)
    # zljlr(主力净流入), zllr(主力流入), zllc(主力流出)
    df_tx = common.safe_call(akshare.stock_zh_a_spot_tx)
    if df_tx is not None:
        for _, row in df_tx.iterrows():
            code, exchange, board = common.cna_code(str(row["code"]))
            data[code] = {
                "code": code,
                "name": common.cna_fixname(row["name"]),
                "security_type": "stock",
                "exchange": exchange,
                "board": board,
            }
    else:
        logging.warning("Tencent quote failed, falling back to stock_info_a_code_name")
        df = common.safe_call(akshare.stock_info_a_code_name)
        if df is None:
            logging.warning("stock_info_a_code_name failed")
        else:
            for _, row in df.iterrows():
                code, exchange, board = common.cna_code(str(row["code"]))
                data[code] = {
                    "code": code,
                    "name": common.cna_fixname(row["name"]),
                    "security_type": "stock",
                    "exchange": exchange,
                    "board": board,
                }

    df_sh = common.safe_call(akshare.stock_info_sh_name_code, symbol="主板A股")
    if df_sh is None:
        logging.warning("stock_info_sh_name_code failed")
    else:
        for _, row in df_sh.iterrows():
            code, __, __ = common.cna_code(str(row["证券代码"]).zfill(6), "sh")
            data[code].update({
                "code": code,
                "name": common.cna_fixname(row["证券简称"]),
                "security_type": "stock",
                "exchange": "SH",
                "board": "Main",
                "list_date": str(row.get("上市日期")).replace("-", ""),
            })
    df_sh = common.safe_call(akshare.stock_info_sh_name_code, symbol="科创板")
    if df_sh is None:
        logging.warning("stock_info_sh_name_code failed")
    else:
        for _, row in df_sh.iterrows():
            code, __, __ = common.cna_code(str(row["证券代码"]).zfill(6), "sh")
            data[code].update({
                "code": code,
                "name": common.cna_fixname(row["证券简称"]),
                "security_type": "stock",
                "exchange": "SH",
                "board": "STAR",
                "list_date": str(row.get("上市日期")).replace("-", ""),
            })

    def _mapboard(board):
        BOARDMAP = { "主板": "Main", "创业板": "ChiNext" }
        if board in BOARDMAP:
            return BOARDMAP[board]
        return board

    df_sz = common.safe_call(akshare.stock_info_sz_name_code, symbol="A股列表")
    if df_sz is None:
        logging.warning("stock_info_sz_name_code failed")
    else:
        for _, row in df_sz.iterrows():
            code, __, __ = common.cna_code(str(row["A股代码"]).zfill(6), "sz")
            data[code].update({
                "code": code,
                "name": common.cna_fixname(row["A股简称"]),
                "security_type": "stock",
                "exchange": "SZ",
                "board": _mapboard(row["板块"]),
                "industry": row.get("所属行业", "").split(" ", 1)[-1],
                "list_date": str(row.get("A股上市日期")).replace("-", ""),
                "share_capital": common.fixint(row["A股总股本"]),
                "tradable_share": common.fixint(row["A股流通股本"]),
            })

    df_bj = common.safe_call(akshare.stock_info_bj_name_code)
    if df_bj is None:
        logging.warning("stock_info_bj_name_code failed")
    else:
        for _, row in df_bj.iterrows():
            code, __, board = common.cna_code(str(row["证券代码"]).zfill(6), "bj")
            data[code].update({
                "code": code,
                "name": common.cna_fixname(row["证券简称"]),
                "security_type": "stock",
                "exchange": "BJ",
                "board": board,
                "industry": row.get("所属行业", None),
                "list_date": str(row.get("上市日期")).replace("-", ""),
                "share_capital": common.fixint(row["总股本"]),
                "tradable_share": common.fixint(row["流通股本"]),
            })

    def corder(code):
        marketorder = { "sh": 0, "sz": 1, "bj": 2 }
        return marketorder[code[:2]], code
    records = sorted(data.values(), key=lambda x:corder(x["code"]))
    logging.info(f"CNA stocks {len(records)}")
    return records


def cna_etf_list():
    records = []
    cats = [
        ("ETF", "ETF基金", "etf"),
        ("LOF", "LOF基金", "lof"),
        # ("Closed-end Fund", "封闭式基金", "cef"), # ignore CEFs
    ]
    for cat in cats:
        logging.info("Fetching CNA %s list...", cat[0])
        df = common.safe_call(akshare.fund_etf_category_sina, symbol=cat[1])
        if df is None:
            logging.warning("fund_etf_category_sina failed")
            return None
        for _, row in df.iterrows():
            code, exchange, __ = common.cna_code(str(row["代码"]))
            records.append({
                "code": code,
                "name": common.cna_fixname(row["名称"]),
                "security_type": cat[2],
                "exchange": exchange,
            })
        logging.info("CNA %ss: %d", cat[0], len(df))
    records.sort(key=lambda x: x["code"])
    return records


def cna_updatedata_daily(symbols):
    stat = [0, 0]   # done_num / all_num
    for symbol in symbols:
        stat[1] += 1
        name = f"{symbol["code"]}:{symbol["name"]}" if "name" in symbol else symbol["code"]
        # determine start date for the symbol
        regdata = common.get_bar("cna", symbol["code"], "daily")
        # print(regdata)
        if len(regdata) > 0:    # if server has data, use newest date+1
            start_date = datetime.strptime(regdata["time"], "%Y%m%d")
            start_date = (start_date + timedelta(days=1)).strftime("%Y%m%d")
        elif "list_date" in symbol and isinstance(symbol["list_date"], str):
            start_date = symbol["list_date"]
        else:
            start_date = "19000101"
        if start_date > _cna_date:
            logging.info("%s already up to date %s", name, _cna_date)
            stat[0] += 1
            continue
        # Try tx first, except for BJ
        apilist = ["tx", "sina"]
        if symbol["security_type"] == "stock" and symbol["exchange"] == "BJ":
            apilist = ["sina", "tx"]
        data = cna_getdata_daily(symbol["code"], name, start_date, _cna_date, apilist[0])
        if data is None:
            data = cna_getdata_daily(symbol["code"], name, start_date, _cna_date, apilist[1])
        # print(len(data))
        # print(data[:10])
        # print(data[-10:])
        # put server
        if data is not None and len(data) > 0:
            common.put_bars(data, "cna")
            if max(d["time"] for d in data) >= _cna_date:
                stat[0] += 1
        logging.info("%s put %d bars", name, len(data) if data is not None else 0)
        # time.sleep(1)
        if start_date[:4] != _cna_date[:4]:
            time.sleep(2)
        # break
    return stat

def cna_getdata_daily(code, name, start_date, end_date, api="tx"):
    # fetch data
    logging.trace("Fetching data %s %s: %s-%s", api, name, start_date, end_date)
    api = akshare.stock_zh_a_daily if api == "sina" else akshare.stock_zh_a_hist_tx
    rawdata = common.safe_call(
            api, symbol=code, start_date=start_date, end_date=end_date, adjust="")
    if rawdata is None:
        logging.warning("Failed %s: %s-%s", name, start_date, end_date)
        return None
    # print(rawdata)
    # for key, val in rawdata.iloc[0].items():
    #     print(key, type(val))
    # prepare put server payload
    data = []
    for __, row in rawdata.iterrows():
        o = common.fixfloat(row["open"])
        h = common.fixfloat(row["high"])
        l = common.fixfloat(row["low"])
        c = common.fixfloat(row["close"])
        d = row["date"].strftime("%Y%m%d")
        if h < o or h < c or h < l:
            logging.warning("%s %s Fix high (%f:%f:%f:%f)", name, d, o, h, l, c)
            h = max(o, c, l)
        if l > o or l > c or l > h:
            logging.warning("%s %s Fix low (%f:%f:%f:%f)", name, d, o, h, l, c)
            l = min(o, c, h)
        if row["date"].isoweekday() >= 6:
            logging.warning("%s %s is weekend %d", name, d, row["date"].isoweekday())
            continue
        data.append({
            "code": code,
            "frequency": "daily",
            "state": "normal",
            "time": d, "open": o, "high": h, "low": l, "close": c,
            "volume": int(row["volume"]),
        })
    return data


def cnof_list():
    logging.info("Fetching CN open-end fund list...")
    records = defaultdict(dict)
    df_name = common.safe_call(akshare.fund_name_em)
    # print(df_name)
    if df_name is not None:
        for _, row in df_name.iterrows():
            code, __, __ = common.cna_code(str(row["基金代码"]), "of")
            records[code] = {
                "code": code,
                "name": common.cna_fixname(row["基金简称"]),
                "security_type": "open_fund",
                "industry": row["基金类型"],
            }
    # 
    df_daily = common.safe_call(akshare.fund_open_fund_daily_em)
    if df_daily is None:
        logging.warning("fund_open_fund_daily_em failed")
        return []
    # parse columns, and get value keys
    vkeys = []
    for key in df_daily.columns.tolist():
        if key.endswith("-累计净值"):
            keydate = key.rsplit("-", 1)[0]
            keydate = keydate.replace("-", "")
            if len(keydate) == 8 and keydate.isdigit():
                vkeys.append((key, keydate))
    # print(df_daily.info())
    # print(df_daily)
    for _, row in df_daily.iterrows():
        code, __, __ = common.cna_code(str(row["基金代码"]), "of")
        values = {}
        for key, keydate in vkeys:
            value = common.fixfloat(row[key])
            if isinstance(value, float):
                values[keydate] = value
        records[code].update({
            "code": code,
            "name": common.cna_fixname(row["基金简称"]),
            "security_type": "open_fund",
            "values": values,
        })
    alllen = len(records)
    records = [v for k, v in records.items() if "values" in v]
    logging.info(f"CN open-end funds: {len(records)}:{alllen}")
    records.sort(key=lambda x: x["code"])
    return records


def cnof_updatedata(symbols):
    stat = [0, 0]   # done_num / all_num
    opendates = sorted(_cna_open_dates) # sorted open dates
    openmap = dict((date, i) for i, date in enumerate(opendates))   # dates -> index in sorted
    allthresh = opendates[0]    # thresh that 3-mon covers
    for symbol in symbols:
        name = f"{symbol["code"]}:{symbol["name"]}" if "name" in symbol else symbol["code"]
        if "values" not in symbol or len(symbol["values"]) == 0:
            logging.info("%s Skip no value", name)
            continue
        stat[1] += 1
        # get server date to determine start date for the symbol
        regdata = common.get_bar("cnof", symbol["code"], "daily")
        newdate = None
        # print(regdata)
        records = []
        period = None
        if len(regdata) > 0:    # if server has data
            newdate = regdata["time"]
            # logging.info("regdata %s", regdata)
            # test whether symbol["values"] are enough
            if (newdate in symbol["values"] or
                    newdate in openmap and openmap[newdate] >= len(openmap) - 1 or
                    newdate in openmap and opendates[openmap[newdate] + 1] in symbol["values"]):
                for date, value in symbol["values"].items():    # add values
                    if date > newdate:
                        records.append({
                            "code": symbol["code"],
                            "frequency": "daily",
                            "state": "normal",
                            "time": date, "open": value, "high": value, "low": value, "close": value,
                            "volume": 1,    # value 1 takes smallest storage for `volume`
                        })
                if len(records) == 0:
                    logging.info("%s already up to date %s", name, newdate)
                    if newdate >= _cna_date:
                        stat[0] += 1
                    continue
            elif newdate >= allthresh:
                period = "3月"
            else:
                period = "成立来"
        else:
            period = "成立来"
        if period is not None:
            logging.trace("Fetching data %s: %s", name, period)
            df = common.safe_call(akshare.fund_open_fund_info_em, symbol["code"][2:], "累计净值走势", period)
            if df is None:
                logging.warning("Failed %s: %s", name, period)
                time.sleep(5)
            else:
                # print(df.info())
                # print(df)
                for __, row in df.iterrows():
                    date = row["净值日期"].strftime("%Y%m%d")
                    if newdate is not None and date <= newdate:
                        continue
                    if row["净值日期"].isoweekday() >= 6:
                        logging.warning("%s %s is weekend %d",
                                        name, date, row["净值日期"].isoweekday())
                        continue
                    value = common.fixfloat(row["累计净值"])
                    if value is None:
                        logging.warning("%s %s invalid value: %s", name, date, row.to_dict())
                        continue
                    records.append({
                        "code": symbol["code"],
                        "frequency": "daily",
                        "state": "normal",
                        "time": date, "open": value, "high": value, "low": value, "close": value,
                        "volume": 1,    # value 1 takes smallest storage for `volume`
                    })
        records.sort(key=lambda x: x["time"])
        # print(len(records), records[0], records[-1], end="\n")

        if records is not None and len(records) > 0:
            common.put_bars(records, "cnof")
            if max(d["time"] for d in records) >= _cna_date:
                stat[0] += 1
        logging.info("%s put %d bars", name, len(records) if records is not None else 0)
        if period is not None:
            time.sleep(1)
    return stat


if __name__ == "__main__":
    sys.exit(main(sys.argv))
