import zlib
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import List, Dict, Any

from core.db import user_ratings_collection
from scraping.scraper import scrape_user, fetch

def _signature_from_pairs(pairs):
    raw = "|".join(f"{t}::{r}" for t, r in pairs)
    return f"{zlib.crc32(raw.encode('utf-8')):08x}"

async def _light_check(username):
    """Return a small fingerprint of page 1 (title+rating pairs)."""
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
    return _signature_from_pairs(pairs)

async def _upsert_user_ratings(username, movies, first_page_sig):
    ratings = []
    for m in movies:
        if not m.get("title"):
            continue
        ratings.append({
            "title": m.get("title"),
            "tmdb_id": m.get("tmdb_id"),
            "imdb_id": m.get("imdb_id"),
            "user_rating": m.get("user_rating"),
            "poster": m.get("poster"),
            "year": m.get("year"),
            "genres": m.get("genres", []),
            "average": m.get("average"),
            "votes": m.get("votes"),
        })
    now = datetime.now(timezone.utc).isoformat()
    result = await user_ratings_collection.update_one( 
        {"lb_username": username},
        {"$set": {
            "ratings": ratings,
            "updated_at": now,
            "first_page_sig": first_page_sig,
            "ratings_count": len(ratings),
            "source": "letterboxd",
        }},
        upsert=True
    )
   
    print(f"[UserRatings upsert] user={username} matched={result.matched_count} "
          f"modified={result.modified_count} upserted_id={result.upserted_id} "
          f"ratings_count={len(ratings)} sig={first_page_sig}")

async def get_user_ratings_or_sync(username, force = False):
    """Return cached ratings unless stale; scrape abd update if stale or forced."""
    cached = await user_ratings_collection.find_one({"lb_username": username})
    if cached and not force:
        try:
            sig_now = await _light_check(username)
            if sig_now and sig_now == cached.get("first_page_sig"):
                print("[Light check] no change detected; using cache")
                return cached.get("ratings", [])
            # ADD: only print when we actually plan to update
            if sig_now and sig_now != cached.get("first_page_sig"):
                print(f"[Light check] change detected for user={username}: "
                      f"{cached.get('first_page_sig')} -> {sig_now}; refreshing…")
            elif not sig_now:
                print(f"[Light check] empty signature for user={username}; treating as stale; refreshing…")
        except Exception as e:
            print(f"[Light check] failed for user={username}: {e}; refreshing…")

    movies = await scrape_user(username)
    if not movies:
        print(f"[Scrape] no movies for user={username}; returning cached if present")
        return cached.get("ratings", []) if cached else []
    sig_now = await _light_check(username)
    await _upsert_user_ratings(username, movies, sig_now)
    return movies
