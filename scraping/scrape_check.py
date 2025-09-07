import zlib
import aiohttp
from bs4 import BeautifulSoup
from scraping.scraper import fetch

def _signature_from_pairs(pairs: list[tuple[str, int]]) -> str:
    raw = "|".join(f"{t}::{r}" for t, r in pairs)
    return f"{zlib.crc32(raw.encode('utf-8')):08x}"

async def light_check(username: str) -> tuple[str, int]:
    """Scrape only page 1; return (first_page_sig, count_on_page1)."""
    url = f"https://letterboxd.com/{username}/films/page/1/"
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, url)
    soup = BeautifulSoup(html, "lxml")
    grid = soup.find(class_="grid")
    pairs = []
    if grid:
        for li in grid.find_all("li", class_="griditem"):
            img = li.find("img")
            title = img["alt"] if img else None
            span = li.find("span", class_="rating")
            if not title or not span:
                continue
            try:
                rating_class = span["class"][-1]
                user_rating = int(rating_class.split("-")[-1])
            except Exception:
                continue
            if user_rating == 0:
                continue
            pairs.append((title, user_rating))
    return _signature_from_pairs(pairs), len(pairs)

async def is_user_stale(username: str, user_doc) -> bool:
    try:
        new_sig, page1_count = await light_check(username)
        old_sig = user_doc.get("first_page_sig")
        if old_sig and new_sig == old_sig:
            # quick exit: no change detected
            print("no change detected")
            return False
        # optional extra check: total count changed a lot
        # (requires scraping page count or storing a known count)
        return True
    except Exception:
        # If light check fails, be conservative
        return True
