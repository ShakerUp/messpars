import unittest

from mexc_announcements import parse_mexc_announcements


class MexcAnnouncementsTests(unittest.TestCase):
    def test_feed_normalization_and_host_check(self):
        payload = {"code": 0, "data": [{"totalPage": "42", "details": [
            {"title": "Dividend settlement", "postTime": 1792162290000,
             "url": "https://www.mexc.com/announcements/article/dividend-123"},
            {"title": "Forged", "postTime": 1792162290001,
             "url": "https://www.mexc.com.evil.example/article"},
        ]}]}
        articles, total_pages = parse_mexc_announcements(payload)
        self.assertEqual(total_pages, 42)
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["source"], "MEXC")
        self.assertEqual(articles[0]["code"], articles[0]["url"])
        self.assertEqual(articles[0]["release_date"], 1792162290000)

    def test_schema_failure_is_not_empty_feed(self):
        with self.assertRaises(ValueError):
            parse_mexc_announcements({"code": 600, "data": []})
        with self.assertRaises(ValueError):
            parse_mexc_announcements({"code": 0, "data": {}})


if __name__ == "__main__":
    unittest.main()
