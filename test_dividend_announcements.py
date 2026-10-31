import unittest
from datetime import date
from dividend_announcements import parse_article, merge_snapshot


class DividendTests(unittest.TestCase):
    def test_gate_english_dates_and_utc_settlement(self):
        html = '<table><tr><td>Futures Market</td><td>Underlying Assets</td><td>Dividend per Share (USD)</td><td>Ex-Dividend Date</td><td>Gate Dividend Settlement Time (UTC)</td></tr><tr><td>BXUSDT</td><td>Blackstone Inc. (BX)</td><td>1.12</td><td>November 2, 2026</td><td>November 2, 2026, 08:00</td></tr></table>'
        record = parse_article({'source': 'Gate', 'url': 'https://www.gate.com/announcements/article/102527'}, html)[0]
        self.assertEqual(record['symbol'], 'BX')
        self.assertEqual(record['amount'], 1.12)
        self.assertEqual(record['ex_date'], '2026-11-02')
        self.assertEqual(record['settlement_at'], '2026-11-02T08:00:00+00:00')

    def test_bitget_settlement_is_not_ex_date_and_multiple_symbols(self):
        html = '<table><tr><td>Futures Pair</td><td>Dividend per Share (USD)</td><td>Ex-Dividend Date (ET)</td><td>Settlement Time (UTC+8)</td></tr>'
        html += '<tr><td>BXUSDT</td><td>1.12</td><td>2026-11-02</td><td>2026-10-31 08:00:00</td></tr>'
        html += '<tr><td>CSTOCKUSDT</td><td>0.67</td><td>2026-11-02</td><td>2026-10-31 08:00:00</td></tr></table>'
        records = parse_article({'source': 'Bitget', 'url': 'https://www.bitget.com/test'}, html)
        self.assertEqual([r['symbol'] for r in records], ['BX', 'C'])
        self.assertEqual(records[0]['settlement_at'], '2026-10-31T00:00:00+00:00')
        self.assertEqual(records[0]['ex_date'], '2026-11-02')
        snapshot = {'events': [], 'symbols': {'BX': ['Bitget', 'Gate']}, 'prices': {'BX': 120}}
        merged = merge_snapshot(snapshot, records, today=date(2026, 10, 31))
        self.assertEqual(len(merged['events']), 2)
        self.assertEqual(merge_snapshot(merged, records, today=date(2026, 10, 31)), merged)
        self.assertEqual(len(merge_snapshot(snapshot, records, ['BX'], date(2026, 10, 31))['events']), 1)

    def test_missing_timezone_does_not_invent_settlement(self):
        html = '<table><tr><th>Symbol</th><th>Dividend per Share USD</th><th>Ex-Date</th><th>Settlement Time</th></tr><tr><td>BX</td><td>1.12</td><td>2026-11-02</td><td>2026-11-02 08:00</td></tr></table>'
        self.assertIsNone(parse_article({'source': 'Gate', 'url': 'https://www.gate.com/test'}, html)[0]['settlement_at'])


if __name__ == '__main__':
    unittest.main()
