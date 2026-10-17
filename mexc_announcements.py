"""Normalize MEXC's public announcement feed for the shared monitor."""

from urllib.parse import urlparse


def parse_mexc_announcements(payload):
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise ValueError("MEXC announcements response failed")
    data = payload.get("data")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        raise ValueError("MEXC announcements schema changed")
    details = data[0].get("details")
    if not isinstance(details, list):
        raise ValueError("MEXC announcements schema changed")
    try:
        total_pages = int(data[0]["totalPage"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("MEXC announcements pagination changed") from exc
    articles = []
    for item in details:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        parsed_url = urlparse(url)
        if (not title or parsed_url.scheme != "https"
                or parsed_url.hostname not in {"www.mexc.com", "mexc.com"}):
            continue
        articles.append({
            "code": url,
            "title": title,
            "url": url,
            "category": "",
            "summary": "",
            "release_date": int(item.get("postTime") or 0),
            "source": "MEXC",
        })
    return articles, total_pages
