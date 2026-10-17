"""Best-effort US equity dividend calendar for exchange-listed USDT futures.

Nasdaq's website JSON endpoints are not a supported data API. Fail closed when
they change or are unavailable; never turn missing data into a zero dividend.
"""

import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone


STATE_FILE = "stock_calendar_state.json"
NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Accept-Language": "en-US,en;q=0.5",
}
EXCHANGES = ("Binance", "Bitget", "Gate", "OKX", "Bybit")
TICKER = re.compile(r"^[A-Z][A-Z0-9.\-]{0,11}$")


def fetch_json(url, headers=None):
    default_headers = NASDAQ_HEADERS if urllib.parse.urlparse(url).hostname == "api.nasdaq.com" else {
        "User-Agent": "message-parser/1.0", "Accept": "application/json"}
    request = urllib.request.Request(url, headers=headers or default_headers)
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def get_json(url, params=None, headers=None):
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return fetch_json(url, headers)


def clean_ticker(value):
    symbol = str(value or "").strip().upper()
    return symbol if TICKER.fullmatch(symbol) else None


def add_symbol(result, exchange, ticker):
    ticker = clean_ticker(ticker)
    if ticker:
        result.setdefault(ticker, set()).add(exchange)


def discover_binance(get=get_json):
    payload = get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    result = {}
    for item in payload.get("symbols", []):
        if item.get("status") != "TRADING" or item.get("quoteAsset") != "USDT":
            continue
        if item.get("contractType") not in {"PERPETUAL", "TRADIFI_PERPETUAL"}:
            continue
        category = str(item.get("underlyingType") or "").upper()
        subtypes = [str(v).upper() for v in item.get("underlyingSubType", [])]
        if category in {"EQUITY", "STOCK"} or any(v in {"EQUITY", "STOCK"} for v in subtypes):
            add_symbol(result, "Binance", item.get("baseAsset"))
    return result


def discover_bitget(get=get_json):
    payload = get("https://api.bitget.com/api/v3/market/instruments", {"category": "USDT-FUTURES"})
    if str(payload.get("code")) != "00000":
        raise ValueError("Bitget instruments response failed")
    result = {}
    for item in payload.get("data", []):
        if (str(item.get("instrumentType", "")).lower() == "stock"
                and str(item.get("status", "")).lower() == "online"
                and str(item.get("marketRegion", "US")).upper() in {"US", "USA"}):
            add_symbol(result, "Bitget", item.get("underlyingTicker") or item.get("baseCoin"))
    return result


def discover_gate(get=get_json):
    contracts = get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
    if not isinstance(contracts, list):
        raise ValueError("Gate futures response changed")
    bases = {str(item.get("name", "")).upper().removesuffix("_USDT")
             for item in contracts if not item.get("in_delisting")}
    result = {}
    page = 1
    while page <= 30:
        payload = get("https://api.gateio.ws/api/v4/stock/symbols",
                      {"exchange": "us", "page": page, "page_size": 500})
        data = payload.get("data", {})
        rows = data.get("list", [])
        for item in rows:
            symbol = clean_ticker(item.get("symbol"))
            if item.get("asset_type") == "STOCK" and symbol in bases:
                add_symbol(result, "Gate", symbol)
        if page >= int(data.get("total_page") or 0):
            break
        page += 1
    else:
        raise ValueError("Gate stock pagination exceeded")
    return result


def discover_okx(get=get_json):
    payload = get("https://www.okx.com/api/v5/public/instruments", {"instType": "SWAP"})
    if str(payload.get("code")) != "0":
        raise ValueError("OKX instruments response failed")
    result = {}
    for item in payload.get("data", []):
        if (str(item.get("instCategory")) == "3" and item.get("state") == "live"
                and item.get("instId", "").endswith("-USDT-SWAP")):
            add_symbol(result, "OKX", item.get("instId", "").split("-")[0])
    return result


def discover_bybit(get=get_json):
    result = {}
    cursor = None
    for _ in range(30):
        params = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        payload = get("https://api.bybit.com/v5/market/instrument-summary", params)
        if payload.get("retCode") != 0:
            raise ValueError("Bybit instrument summary failed")
        data = payload.get("result", {})
        for item in data.get("list", []):
            if (item.get("marketRegion") == "US" and str(item.get("symbolType", "")).lower() == "stock"
                    and item.get("settleCoin") == "USDT"
                    and item.get("underlyingTicker")):
                add_symbol(result, "Bybit", item["underlyingTicker"])
        next_cursor = data.get("nextPageCursor")
        if not next_cursor:
            return result
        if next_cursor == cursor:
            raise ValueError("Bybit pagination did not advance")
        cursor = next_cursor
    raise ValueError("Bybit instrument pagination exceeded")


def discover_all(get=get_json):
    result = {}
    errors = {}
    for name, discover in (
        ("Binance", lambda: discover_binance(get)),
        ("Bitget", lambda: discover_bitget(get)),
        ("Gate", lambda: discover_gate(get)),
        ("OKX", lambda: discover_okx(get)),
        ("Bybit", lambda: discover_bybit(get)),
    ):
        try:
            found = discover()
            if not found:
                errors[name] = "Не подтверждены фондовые USDT-контракты"
            for symbol, exchanges in found.items():
                result.setdefault(symbol, set()).update(exchanges)
        except Exception as exc:
            errors[name] = str(exc)[:160]
    return result, errors


def parse_number(value):
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        match = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?", str(value or ""))
        if not match:
            return None
        number = float(match.group(0).replace(",", ""))
    return number if 0 < number < 1_000_000 else None


def parse_date(value):
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(value), fmt).date()
        except ValueError:
            pass
    return None


def parse_nasdaq_rows(payload, day):
    status = payload.get("status", {}) if isinstance(payload, dict) else {}
    if (status.get("rCode") == 200 and any(
            item.get("code") == 1002 and "No record found" in item.get("errorMessage", "")
            for item in status.get("bCodeMessage") or [])):
        return []
    data = payload.get("data") if isinstance(payload, dict) else None
    calendar = data.get("calendar") if isinstance(data, dict) else None
    if (calendar is None and isinstance(data, dict)
            and payload.get("status", {}).get("rCode") == 200
            and str(payload.get("message", "")).startswith("There are no dividends")):
        return []
    if (isinstance(calendar, dict) and calendar.get("rows") is None
            and payload.get("status", {}).get("rCode") == 200
            and str(payload.get("message", "")).startswith("There are no dividends")):
        return []
    if not isinstance(calendar, dict) or not isinstance(calendar.get("rows"), list):
        raise ValueError("Nasdaq dividend calendar unavailable or schema changed")
    events = []
    for row in calendar["rows"]:
        symbol = clean_ticker(row.get("symbol"))
        ex_date = parse_date(row.get("dividend_Ex_Date") or row.get("exOrEffDate") or row.get("exDate"))
        amount = parse_number(row.get("dividend_Rate") or row.get("dividendRate") or row.get("amount"))
        if symbol and ex_date == day and amount:
            events.append({"symbol": symbol, "ex_date": day.isoformat(), "amount": amount,
                           "currency": "USD", "source": "Nasdaq"})
    return events


def fetch_dividends(start, end, get=get_json):
    events = []
    day = start
    while day <= end:
        if day.weekday() < 5:
            payload = get("https://api.nasdaq.com/api/calendar/dividends", {"date": day.isoformat()})
            try:
                events.extend(parse_nasdaq_rows(payload, day))
            except ValueError as exc:
                raise ValueError(f"Nasdaq {day.isoformat()}: {exc}") from exc
            time.sleep(0.15)
        day += timedelta(days=1)
    return events


def fetch_prices(get=get_json):
    payload = get("https://api.nasdaq.com/api/screener/stocks",
                  {"tableonly": "true", "limit": 10000})
    data = payload.get("data") if isinstance(payload, dict) else None
    table = data.get("table") if isinstance(data, dict) else None
    if not isinstance(table, dict) or not isinstance(table.get("rows"), list):
        raise ValueError("Nasdaq stock screener unavailable or schema changed")
    return {symbol: parse_number(row.get("lastsale")) for row in table["rows"]
            if (symbol := clean_ticker(row.get("symbol")))}


def build_snapshot(today=None, get=get_json):
    today = today or date.today()
    symbols, errors = discover_all(get)
    if not symbols:
        raise ValueError("Не удалось подтвердить ни одного фондового USDT-контракта: " + str(errors))
    end = date(today.year, 12, 31)
    events = fetch_dividends(today, end, get) if today <= end else []
    prices = fetch_prices(get)
    matched = {}
    for event in events:
        symbol = event["symbol"]
        # The stock screener excludes ETFs, which Binance labels EQUITY too.
        if symbol not in symbols or symbol not in prices:
            continue
        price = prices.get(symbol)
        enriched = {**event, "exchanges": sorted(symbols[symbol]), "price": price,
                    "event_yield_pct": round(event["amount"] / price * 100, 4) if price else None}
        matched[(symbol, event["ex_date"])] = enriched
    return {"checked_at": datetime.now(timezone.utc).isoformat(),
            "symbols": {symbol: sorted(exchanges) for symbol, exchanges in symbols.items()},
            "errors": errors, "events": sorted(matched.values(), key=lambda row: (row["ex_date"], row["symbol"])),
            "calendar_end": end.isoformat(), "price_source": "Nasdaq last sale"}


def load_state(path=STATE_FILE):
    try:
        with open(path, encoding="utf-8") as stream:
            state = json.load(stream)
        return state if isinstance(state, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(state, path=STATE_FILE):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as stream:
        json.dump(state, stream, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def due_reminders(snapshot, sent, today):
    if not snapshot.get("checked_at") or not snapshot.get("events"):
        return []
    return [event for event in snapshot["events"]
            if parse_date(event.get("ex_date")) == today + timedelta(days=3)
            and f'{event["symbol"]}:{event["ex_date"]}' not in sent]
