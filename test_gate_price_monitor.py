import unittest
import asyncio
import copy
from datetime import timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import gate_price_monitor as monitor


class GatePriceTests(unittest.TestCase):
    def contract(self, **extra):
        return {"status": "trading", "contract_type": "", **extra}

    def test_formula_uses_mark_as_denominator_and_both_directions(self):
        rows = [{"contract": "A_USDT", "last": "120", "mark_price": "100"},
                {"contract": "B_USDT", "last": "80", "mark_price": "100"}]
        found = monitor.signals(rows, {"A_USDT": self.contract(), "B_USDT": self.contract()}, Decimal(20), [])
        self.assertEqual([s["deviation"] for s in found], [Decimal(20), Decimal(-20)])
        self.assertTrue(all(s["triggered"] for s in found))

    def test_blacklist_and_invalid_prices_and_delisting(self):
        rows = [{"contract": name, "last": last, "mark_price": mark} for name, last, mark in (
            ("A_USDT", "NaN", "1"), ("B_USDT", "1", "0"), ("C_USDT", "2", "1"),
            ("D_USDT", "2", "1"), ("E_USDT", "Infinity", "1"))]
        contracts = {row["contract"]: self.contract() for row in rows}
        contracts["D_USDT"].update(in_delisting=True, status="delisted")
        self.assertEqual(monitor.signals(rows, contracts, Decimal(5), ["C_USDT"]), [])
        self.assertEqual(monitor.parse_blacklist("btc, BTCUSDT ETH_USDT"), ["BTC_USDT", "ETH_USDT"])
        self.assertEqual(monitor.parse_blacklist("0"), [])
        with self.assertRaises(ValueError):
            monitor.parse_blacklist("<bad>")

    def test_delisting_contracts_are_scanned_until_delisted(self):
        contracts = {"AGENCY_USDT": self.contract(status="delisting", in_delisting=True)}
        rows = [{"contract": "AGENCY_USDT", "last": "0.000076", "mark_price": "0.000061"}]
        found = monitor.signals(rows, contracts, Decimal(20), [])
        self.assertEqual(len(found), 1)
        self.assertTrue(found[0]["triggered"])
        self.assertIn("делистинга", monitor.alert_text(found[0], "now"))
        contracts["AGENCY_USDT"]["status"] = "delisted"
        self.assertEqual(monitor.signals(rows, contracts, Decimal(20), []), [])

    def test_cooldown_reentry_and_direction_change(self):
        signal = {"triggered": True, "deviation": Decimal(20)}
        previous = {"sent_at": 100, "direction": 1, "deviation": "20"}
        self.assertFalse(monitor.should_alert(signal, previous, 101))
        self.assertFalse(monitor.should_alert(signal, previous, 999))
        self.assertTrue(monitor.should_alert(signal, previous, 1000))
        self.assertTrue(monitor.should_alert(signal, None, 101))
        self.assertFalse(monitor.should_alert({**signal, "deviation": Decimal(-20)}, previous, 101))
        self.assertTrue(monitor.should_alert({**signal, "deviation": Decimal(-20)}, previous, 103))
        self.assertFalse(monitor.should_alert({**signal, "triggered": False}, None, 101))

    def test_growth_is_ten_percentage_points(self):
        previous = {"sent_at": 100, "direction": -1, "deviation": "50"}
        signal = {"triggered": True, "deviation": Decimal(-60)}
        self.assertFalse(monitor.should_alert(signal, previous, 102.999))
        self.assertTrue(monitor.should_alert(signal, previous, 103))
        self.assertFalse(monitor.should_alert({**signal, "deviation": Decimal("-59.999")}, previous, 103))
        self.assertFalse(monitor.should_alert({**signal, "deviation": Decimal(-19)}, previous, 103))

    def test_asset_type_and_missing_limits(self):
        signal = {"contract": "AAPL_USDT", "last": Decimal(110), "mark": Decimal(100),
                  "deviation": Decimal(10), "metadata": self.contract(contract_type="stocks")}
        text = monitor.alert_text(signal, "13.11 12:00:00")
        self.assertIn("RWA · акции", text)
        self.assertIn("+10.00%", text)
        self.assertIn("нет данных", text)
        signal["metadata"] = self.contract()
        self.assertIn("Крипта", monitor.alert_text(signal, "now"))
        signal["metadata"] = {}
        self.assertIn("Тип не определён", monitor.alert_text(signal, "now"))

    def test_position_limit_matches_displayed_leverage(self):
        tiers = [{"leverage_max": "5", "risk_limit": "5000"},
                 {"leverage_max": "4", "risk_limit": "7000"},
                 {"leverage_max": "1.01", "risk_limit": "2000000"}]
        self.assertEqual(monitor.limit_for_leverage(tiers, "5"), Decimal(5000))
        self.assertEqual(monitor.limit_for_leverage(tiers, "4"), Decimal(7000))
        self.assertIsNone(monitor.limit_for_leverage(tiers, "6"))
        signal = {"contract": "SPACEHOOD_USDT", "last": Decimal("0.0012"), "mark": Decimal("0.000822"),
                  "deviation": Decimal("45.99"), "metadata": {"contract_type": "", "leverage_max": "5",
                  "position_limit": "5000", "risk_limit_max": "2000000"}}
        text = monitor.alert_text(signal, "now")
        self.assertIn("\n\nМакс. позиция (5x): 5 000 $", text)
        self.assertNotIn("Макс. плечо:", text)
        self.assertNotIn("2 000 000", text)
        self.assertNotIn("Gate ·", text)
        self.assertIn("SPACEHOODUSDT", text)


class GatePriceWorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        limiter = patch.object(monitor, "fetch_position_limit", new=AsyncMock(return_value=None))
        limiter.start()
        self.addCleanup(limiter.stop)

    def book(self):
        book = monitor.AlertBook({})
        book.configure({"gate_price_enabled": True, "gate_price_target_chat_id": -100123,
                        "gate_price_threshold_pct": 5, "gate_price_blacklist": []})
        return book

    def signal(self, deviation):
        return {"contract": "BTC_USDT", "deviation": Decimal(str(deviation)),
                "triggered": abs(deviation) >= 5, "last": Decimal(150), "mark": Decimal(100), "metadata": {}}

    async def test_500_ticks_are_coalesced_into_three_notifications(self):
        book = self.book()
        clock = [100.0]
        send = AsyncMock()
        with patch.object(monitor.time, "time", side_effect=lambda: clock[0]), patch.object(monitor, "save_state"):
            book.update([self.signal(50)], clock[0])
            await monitor.dispatch_alerts(book, send, timezone.utc)
            for i in range(1, 501):
                clock[0] = 100 + i / 100
                book.update([self.signal(50 + 4950 * i / 500)], clock[0])
                await monitor.dispatch_alerts(book, send, timezone.utc)
            clock[0] = 106
            await monitor.dispatch_alerts(book, send, timezone.utc)
        self.assertEqual(send.await_count, 3)
        self.assertEqual(len(book.latest), 1)
        self.assertIn("+5000.00%", send.await_args.args[1])

    async def test_recovery_reset_and_restart_cooldown(self):
        book = self.book()
        book.update([self.signal(50)], 100)
        book.acknowledge(book.latest["BTC_USDT"], 0, 100)
        restored = monitor.AlertBook(copy.deepcopy(book.state))
        restored.configure(book.settings)
        restored.update([self.signal(55)], 103)
        self.assertEqual(restored.candidates(103), [])
        restored.update([self.signal(4.9)], 104)
        restored.update([self.signal(5)], 105)
        self.assertEqual(len(restored.candidates(105)), 1)
        self.assertEqual(len(restored.candidates(101)), 0)

    async def test_failed_send_is_retryable_and_stale_prices_not_sent(self):
        book = self.book()
        book.update([self.signal(50)], 100)
        self.assertEqual(book.candidates(131), [])
        with patch.object(monitor.time, "time", return_value=100), patch.object(monitor, "save_state"):
            with self.assertRaises(RuntimeError):
                await monitor.dispatch_alerts(book, AsyncMock(side_effect=RuntimeError("send failed")), timezone.utc)
        self.assertEqual(book.state["alerts"], {})

    async def test_ticks_during_send_are_preserved_without_building_queue(self):
        book = self.book()
        book.update([self.signal(50)], 100)
        async def send_during_burst(_settings, _text):
            for value in range(60, 5001, 10):
                book.update([self.signal(value)], 100)
        with patch.object(monitor.time, "time", return_value=100), patch.object(monitor, "save_state"):
            await monitor.dispatch_alerts(book, send_during_burst, timezone.utc)
        self.assertEqual(len(book.latest), 1)
        self.assertEqual(book.latest['BTC_USDT']['deviation'], Decimal(5000))
        self.assertEqual(book.candidates(102), [])
        self.assertEqual(book.candidates(103)[0]['deviation'], Decimal(5000))


if __name__ == "__main__":
    unittest.main()
