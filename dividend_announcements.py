"""Extract explicit dividend table rows; never infer payments from listing status."""

import re
import urllib.request
import urllib.parse
from urllib.error import HTTPError
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser

from stock_calendar import clean_ticker, parse_date, parse_number

STATE_FILE = "dividend_announcements_state.json"


class Tables(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
            self.cell = None
        elif tag in {"td", "th"} and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in {"td", "th"} and self.cell is not None:
            self.row.append(" ".join(" ".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row:
            self.rows.append(self.row)
            self.row = None


def fetch_article(url):
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in {
            "www.bitget.com", "www.gate.com", "www.mexc.com", "www.binance.com"}:
        raise ValueError("Unsupported announcement host")
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.read(4 * 1024 * 1024).decode("utf-8")
    except HTTPError as exc:
        if exc.code != 403 or parsed.hostname != "www.gate.com":
            raise
        try:
            from curl_cffi import requests
        except ImportError as missing:
            raise RuntimeError("Gate HTTP 403: install requirements-dividends.txt") from missing
        response = requests.get(url, impersonate="chrome", timeout=20)
        response.raise_for_status()
        return response.text


def announcement_date(value):
    parsed = parse_date(value)
    if parsed:
        return parsed
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            pass
    return None


def settlement_datetime(value):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%B %d, %Y, %H:%M", "%B %d, %Y %H:%M", "%b %d, %Y, %H:%M"):
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            pass
    return None


def parse_article(article, html):
    parser = Tables()
    parser.feed(html)
    headers = None
    records = []
    for row in parser.rows:
        lower = [cell.casefold() for cell in row]
        if any("dividend" in c for c in lower) and any("date" in c or "time" in c for c in lower):
            headers = lower
            continue
        if not headers or len(row) != len(headers):
            continue
        def field(predicate):
            return next((row[i] for i, heading in enumerate(headers) if predicate(heading)), "")
        raw_symbol = field(lambda h: any(k in h for k in ("pair", "symbol", "ticker", "contract", "stock", "futures market")))
        symbol = clean_ticker(raw_symbol.upper().replace("_USDT", "").removesuffix("USDT").removesuffix("STOCK"))
        amount = parse_number(field(lambda h: "dividend" in h and "usd" in h))
        ex_raw = field(lambda h: "ex" in h and "date" in h)
        ex_match = re.search(r"\d{4}-\d{2}-\d{2}", ex_raw)
        ex_date = announcement_date(ex_match[0] if ex_match else ex_raw)
        if not symbol or not amount or not ex_date:
            continue
        settlement = None
        for i, heading in enumerate(headers):
            if not any(k in heading for k in ("settlement", "distribution", "snapshot")):
                continue
            moment = settlement_datetime(row[i])
            zone = re.search(r"utc\s*([+-]\d{1,2})?", heading)
            if moment and zone:
                offset = int(zone[1] or 0)
                settlement = moment.replace(
                    tzinfo=timezone(timedelta(hours=offset))).astimezone(timezone.utc).isoformat()
        records.append({"symbol": symbol, "ex_date": ex_date.isoformat(), "amount": amount,
                        "currency": "USD", "source": article["source"], "url": article["url"],
                        "settlement_at": settlement, "release_date": article.get("release_date", 0)})
    return records


def merge_snapshot(snapshot, records, blacklist=(), today=None):
    from datetime import date
    today = today or date.today()
    events = {(e["symbol"], e["ex_date"]): {**e, "confirmations": {}} for e in snapshot.get("events", [])
              if e["symbol"] not in blacklist and e["ex_date"] >= today.isoformat()}
    for record in sorted(records, key=lambda r: r.get("release_date", 0)):
        if record["symbol"] in blacklist or record["ex_date"] < today.isoformat():
            continue
        key = record["symbol"], record["ex_date"]
        event = events.setdefault(key, {
            "symbol": record["symbol"], "ex_date": record["ex_date"], "amount": record["amount"],
            "currency": "USD", "source": "Exchange announcement", "price": None,
            "event_yield_pct": None, "exchanges": snapshot.get("symbols", {}).get(record["symbol"], []),
            "confirmations": {},
        })
        event["confirmations"][record["source"]] = record
        price = snapshot.get("prices", {}).get(record["symbol"]) or event.get("price")
        if price:
            event["price"] = price
            event["event_yield_pct"] = round(event["amount"] / price * 100, 4)
    return {**snapshot, "events": sorted(events.values(), key=lambda e: (e["ex_date"], e["symbol"]))}
