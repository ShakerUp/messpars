import unittest
from datetime import date
from unittest.mock import patch

import stock_calendar as calendar


class StockCalendarTests(unittest.TestCase):
    def test_binance_only_explicit_equity_usdt_perpetuals(self):
        payload = {"symbols": [
            {"status": "TRADING", "quoteAsset": "USDT", "contractType": "TRADIFI_PERPETUAL",
             "underlyingType": "EQUITY", "baseAsset": "AAPL"},
            {"status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL",
             "underlyingType": "COIN", "baseAsset": "AAPL"},
            {"status": "BREAK", "quoteAsset": "USDT", "contractType": "PERPETUAL",
             "underlyingType": "EQUITY", "baseAsset": "MSFT"},
        ]}
        self.assertEqual(calendar.discover_binance(lambda *_args, **_kwargs: payload),
                         {"AAPL": {"Binance"}})

    def test_nasdaq_amount_is_event_not_annual_dividend(self):
        payload = {"data": {"calendar": {"rows": [
            {"symbol": "AAPL", "dividend_Ex_Date": "10/20/2026",
             "dividend_Rate": 0.25, "indicated_Annual_Dividend": 1.0},
            {"symbol": "MSFT", "dividend_Ex_Date": "10/21/2026",
             "dividend_Rate": 0.5},
        ]}}}
        self.assertEqual(calendar.parse_nasdaq_rows(payload, date(2026, 10, 20)), [
            {"symbol": "AAPL", "ex_date": "2026-10-20", "amount": 0.25,
             "currency": "USD", "source": "Nasdaq"}
        ])

    def test_bybit_pagination_and_stock_filter(self):
        responses = [
            {"retCode": 0, "result": {"list": [
                {"symbolType": "stock", "marketRegion": "US", "settleCoin": "USDT",
                 "underlyingTicker": "AAPL"},
                {"symbolType": "ETF", "marketRegion": "US", "settleCoin": "USDT",
                 "underlyingTicker": "QQQ"},
            ], "nextPageCursor": "next"}},
            {"retCode": 0, "result": {"list": [
                {"symbolType": "stock", "marketRegion": "US", "settleCoin": "USDT",
                 "underlyingTicker": "MSFT"},
            ], "nextPageCursor": ""}},
        ]
        calls = []

        def get(_url, params):
            calls.append(params)
            return responses.pop(0)

        self.assertEqual(calendar.discover_bybit(get),
                         {"AAPL": {"Bybit"}, "MSFT": {"Bybit"}})
        self.assertEqual(calls[1]["cursor"], "next")

    def test_nasdaq_schema_failure_is_not_empty_calendar(self):
        with self.assertRaises(ValueError):
            calendar.parse_nasdaq_rows({"data": {"calendar": {}}}, date(2026, 10, 20))

    def test_nasdaq_explicit_empty_responses(self):
        day = date(2026, 11, 3)
        self.assertEqual(calendar.parse_nasdaq_rows({
            "data": {"calendar": {"rows": None}},
            "message": "There are no dividends to show for this date.",
            "status": {"rCode": 200}}, day), [])
        self.assertEqual(calendar.parse_nasdaq_rows({
            "data": None, "status": {"rCode": 200, "bCodeMessage": [
                {"code": 1002, "errorMessage": "Dividend Calendar: No record found."}]}}, day), [])

    def test_snapshot_intersection_and_event_yield(self):
        with patch.object(calendar, "discover_all", return_value=(
            {"AAPL": {"Binance", "Bybit"}, "TSLA": {"OKX"}}, {})), \
             patch.object(calendar, "fetch_dividends", return_value=[
                 {"symbol": "AAPL", "ex_date": "2026-10-20", "amount": 0.5,
                  "currency": "USD", "source": "Nasdaq"},
                 {"symbol": "KO", "ex_date": "2026-10-21", "amount": 0.5,
                  "currency": "USD", "source": "Nasdaq"}]), \
             patch.object(calendar, "fetch_prices", return_value={"AAPL": 100.0}):
            result = calendar.build_snapshot(date(2026, 10, 17))
        self.assertEqual(len(result["events"]), 1)
        self.assertEqual(result["events"][0]["event_yield_pct"], 0.5)
        self.assertEqual(result["events"][0]["exchanges"], ["Binance", "Bybit"])

    def test_reminder_only_three_days_before_once(self):
        snapshot = {"checked_at": "2026-10-17T00:00:00+00:00", "events": [
            {"symbol": "AAPL", "ex_date": "2026-10-20"},
            {"symbol": "MSFT", "ex_date": "2026-10-21"},
        ]}
        self.assertEqual([e["symbol"] for e in calendar.due_reminders(
            snapshot, set(), date(2026, 10, 17))], ["AAPL"])
        self.assertEqual(calendar.due_reminders(
            snapshot, {"AAPL:2026-10-20"}, date(2026, 10, 17)), [])


if __name__ == "__main__":
    unittest.main()
