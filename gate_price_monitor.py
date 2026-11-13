"""Public Gate USDT perpetual mark/last-price monitor."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from html import escape
import logging
import json
import re
import time

from stock_calendar import get_json, load_state, save_state

STATE_FILE = "gate_price_monitor_state.json"
BASE_URL = "https://api.gateio.ws/api/v4/futures/usdt"
WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
COOLDOWN_SECONDS = 15 * 60
MIN_ALERT_SECONDS = 3
GROWTH_POINTS = Decimal("10")
HEALTH = {}
logger = logging.getLogger(__name__)
RWA_TYPES = {"stocks": "акции", "metals": "металлы", "indices": "индексы",
             "forex": "валюты", "commodities": "сырьё", "rwa": "RWA"}


def number(value):
    try:
        result = Decimal(str(value))
        return result if result.is_finite() and result > 0 else None
    except InvalidOperation:
        return None


def normalize_contract(value):
    value = value.strip().upper()
    if value.endswith("_USDT"):
        base = value[:-5]
    elif value.endswith("USDT"):
        base = value[:-4]
    else:
        base = value
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,39}", base):
        raise ValueError("Некорректный тикер")
    return base + "_USDT"


def parse_blacklist(text):
    return sorted({normalize_contract(v) for v in re.split(r"[,\s]+", text.strip()) if v}) if text.strip() != "0" else []


def signals(tickers, contracts, threshold, blacklist):
    result = []
    for ticker in tickers:
        name = ticker.get("contract", "")
        contract = contracts.get(name)
        if not contract or name in blacklist or not re.fullmatch(r"[A-Z0-9._-]+_USDT", name):
            continue
        if contract.get("in_delisting") or contract.get("status", "trading") != "trading":
            continue
        last, mark = number(ticker.get("last")), number(ticker.get("mark_price"))
        if last is None or mark is None:
            continue
        deviation = (last / mark - 1) * 100
        result.append({"contract": name, "last": last, "mark": mark, "deviation": deviation,
                       "triggered": abs(deviation) >= threshold, "metadata": contract})
    return sorted(result, key=lambda s: abs(s["deviation"]), reverse=True)


def should_alert(signal, previous, now):
    if not signal["triggered"]:
        return False
    if not previous:
        return True
    elapsed = now - previous.get("sent_at", 0)
    if elapsed < MIN_ALERT_SECONDS:
        return False
    if previous.get("active") is False:
        return True
    previous_deviation = number(previous.get("deviation"))
    increased = previous_deviation is not None and abs(signal["deviation"]) >= previous_deviation + GROWTH_POINTS
    return (increased or previous.get("direction") != (1 if signal["deviation"] > 0 else -1)
            or elapsed >= COOLDOWN_SECONDS)


def alert_text(signal, detected_at):
    metadata = signal["metadata"]
    category = metadata.get("contract_type")
    kind = "RWA · " + RWA_TYPES[category] if category in RWA_TYPES else (
        "Крипта" if category in {"", "crypto"} else "Тип не определён")
    leverage, risk = number(metadata.get("leverage_max")), number(metadata.get("risk_limit_max"))
    leverage_text = f"{leverage:g}x" if leverage else "нет данных"
    risk_text = f"{risk:,.0f} USDT".replace(",", " ") if risk else "нет данных"
    name = signal["contract"]
    return (
        f'<b>Gate · {signal["deviation"]:+.2f}%</b>\n'
        f'<a href="https://www.gate.com/futures/USDT/{escape(name, quote=True)}">{escape(name)}</a> · {kind}\n\n'
        f'Цена: <b>{signal["last"]:f}</b>\n'
        f'Справедливая: <b>{signal["mark"]:f}</b>\n'
        f'Макс. плечо: {leverage_text}\n'
        f'Макс. лимит риска: {risk_text}\n\n'
        f'Обнаружено: {detected_at} (Киев)'
    )


class AlertBook:
    def __init__(self, state):
        self.state = state
        self.latest = {}
        self.episodes = {}
        self.settings = {}
        self.threshold = None
        self.dirty = False

    def configure(self, settings):
        threshold = number(settings.get("gate_price_threshold_pct"))
        config = [settings.get("gate_price_target_chat_id"), settings.get("gate_price_target_topic_id"), str(threshold)]
        if config != self.state.get("config"):
            self.state = {"config": config, "alerts": {}}
            self.latest.clear()
            self.dirty = True
        self.settings, self.threshold = settings, threshold
        blocked = set(settings.get("gate_price_blacklist", []))
        for name in blocked:
            self.latest.pop(name, None)
            if name in self.state.setdefault("alerts", {}):
                del self.state["alerts"][name]
                self.dirty = True

    @property
    def enabled(self):
        return self.settings.get("gate_price_enabled") and self.settings.get("gate_price_target_chat_id") is not None and self.threshold is not None

    def update(self, found, now):
        for signal in found:
            name = signal["contract"]
            old = self.latest.get(name)
            if not signal["triggered"]:
                if old and old["triggered"]:
                    self.episodes[name] = self.episodes.get(name, 0) + 1
                previous = self.state.setdefault("alerts", {}).get(name)
                if previous and previous.get("active") is not False:
                    previous["active"] = False
                    self.dirty = True
            # Coalesce ticks: there is only one latest value per contract, no alert queue.
            self.latest[name] = {**signal, "received_at": now}

    def candidates(self, now):
        return sorted((s for s in self.latest.values() if now - s["received_at"] < 30
                       and should_alert(s, self.state.setdefault("alerts", {}).get(s["contract"]), now)),
                      key=lambda s: abs(s["deviation"]), reverse=True)

    def acknowledge(self, signal, episode, now):
        name = signal["contract"]
        current = self.latest.get(name)
        self.state.setdefault("alerts", {})[name] = {
            "sent_at": now, "direction": 1 if signal["deviation"] > 0 else -1,
            "deviation": str(abs(signal["deviation"])),
            "active": bool(current and current["triggered"] and self.episodes.get(name, 0) == episode),
        }
        self.dirty = True


async def receive_prices(book):
    failures = 0
    while True:
        try:
            if not book.enabled:
                await asyncio.sleep(0.5)
                continue
            from websockets.asyncio.client import connect
            rows = await asyncio.to_thread(get_json, BASE_URL + "/contracts")
            if not isinstance(rows, list) or not rows:
                raise ValueError("Gate contracts unavailable")
            contracts = {r["name"]: r for r in rows if r.get("status", "trading") == "trading" and not r.get("in_delisting")}
            if not contracts:
                raise ValueError("Gate returned no active contracts")
            async with connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=8 * 1024 * 1024) as ws:
                names = sorted(contracts)
                for start in range(0, len(names), 100):
                    await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.tickers",
                                              "event": "subscribe", "payload": names[start:start + 100]}))
                logger.info("[GATE PRICE WS] Подписка: %s контрактов", len(names))
                quotes = {}
                connected_at = time.monotonic()
                while book.enabled and time.monotonic() - connected_at < 900:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    message = json.loads(raw)
                    if message.get("error"):
                        raise ValueError(str(message["error"]))
                    if message.get("channel") != "futures.tickers" or message.get("event") != "update":
                        continue
                    result = message.get("result", [])
                    result = [result] if isinstance(result, dict) else result
                    updated = []
                    for row in result:
                        name = row.get("contract")
                        if name not in contracts:
                            continue
                        previous = quotes.get(name, {})
                        if row.get("t", 0) < previous.get("t", 0):
                            continue
                        quotes[name] = {**previous, **row}
                        updated.append(quotes[name])
                    book.update(signals(updated, contracts, book.threshold, set(book.settings.get("gate_price_blacklist", []))), time.time())
                    HEALTH.update(last_scan=datetime.now(timezone.utc).isoformat(), checked=len(quotes), error=None)
                    failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            HEALTH["error"] = str(exc)[:180]
            logger.exception("[GATE PRICE WS] Переподключение")
            book.latest.clear()
            await asyncio.sleep(min(60, 2 ** min(failures, 5)))
        finally:
            book.latest.clear()


async def dispatch_alerts(book, send, kyiv_zone):
    if not book.enabled:
        return
    for signal in book.candidates(time.time())[:10]:
        name = signal["contract"]
        # An earlier Telegram send may have yielded while newer ticks arrived.
        current = book.latest.get(name)
        if current is not signal or not should_alert(signal, book.state["alerts"].get(name), time.time()):
            continue
        episode = book.episodes.get(name, 0)
        config = book.state.get("config")
        await send(book.settings, alert_text(signal, datetime.now(kyiv_zone).strftime("%d.%m %H:%M:%S")))
        if book.state.get("config") == config:
            book.acknowledge(signal, episode, time.time())
            save_state(book.state, STATE_FILE)
            book.dirty = False
        logger.info("[GATE PRICE ALERT] %s deviation=%s%%", name, signal["deviation"])


async def monitor(load_settings, send, kyiv_zone):
    book = AlertBook(load_state(STATE_FILE))
    book.configure(load_settings())
    receiver = asyncio.create_task(receive_prices(book))
    try:
        while True:
            try:
                book.configure(load_settings())
                await dispatch_alerts(book, send, kyiv_zone)
                if book.dirty:
                    save_state(book.state, STATE_FILE)
                    book.dirty = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                HEALTH["error"] = str(exc)[:180]
                logger.exception("[GATE PRICE SEND] Ошибка отправки")
                await asyncio.sleep(3)
            await asyncio.sleep(0.1)
    finally:
        receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
