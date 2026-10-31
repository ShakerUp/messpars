"""Exercise the worker without importing the live Telegram app and its credentials."""
import ast
import asyncio
import copy
import logging
from pathlib import Path
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_archive_resumes_after_restart_and_keeps_head_polling(self):
        state = {}
        calls = []

        async def page(source, cursor=None):
            number = cursor or 1
            calls.append((source, number))
            article = {'code': str(number), 'source': source, 'url': f'https://example.test/{source}/{number}',
                       'title': 'Dividend settlement', 'release_date': number}
            return [article], number + 1 if number < 12 else None

        def save(value, _path):
            state.clear()
            state.update(copy.deepcopy(value))

        def parse(article, _html):
            return [{'url': article['url'], 'source': article['source'], 'symbol': 'BX',
                     'amount': 1.12, 'ex_date': '2099-11-02'}]

        namespace = {'load_stock_calendar_state': lambda _path: copy.deepcopy(state),
                     'save_stock_calendar_state': save, 'DIVIDEND_ANNOUNCEMENTS_STATE_FILE': 'test.json',
                     'STOCK_CALENDAR_REFRESH_SECONDS': 28800, 'time': time, 'asyncio': asyncio,
                     'dividend_history_page': page, 'fetch_dividend_article': lambda _url: '',
                     'parse_dividend_article': parse, 'logger': logging.getLogger('test'),
                     'datetime': datetime, 'ZoneInfo': lambda _name: timezone.utc}
        tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8'))
        worker = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'sync_dividend_announcements')
        exec(compile(ast.Module(body=[worker], type_ignores=[]), 'app.py', 'exec'), namespace)
        with patch.object(asyncio, 'sleep', new=AsyncMock()):
            await namespace['sync_dividend_announcements'](True)
            self.assertEqual(state['history']['Gate']['cursor'], 7)
            self.assertEqual(len(state['records']), 24)
            await namespace['sync_dividend_announcements'](True)
        self.assertEqual(state['history']['Gate']['cursor'], 12)
        self.assertEqual(calls.count(('Gate', 1)), 2)
        self.assertEqual(calls.count(('Gate', 7)), 1)
        self.assertEqual(len(state['records']), 44)


if __name__ == '__main__':
    unittest.main()
