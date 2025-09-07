from __future__ import annotations
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from core.db import collection, user_ratings_collection
from scraping.scraper import scrape_user
from services.scoring import calculate_hotness
from services.recommender import load_catalog, recommend_from_ratings
from services.ratings_service import get_user_ratings_or_sync

app = FastAPI(title="Reel Hot Takes API")

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:8000",
    "https://reelhottakes.xyz",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "Welcome to the Reel Hot Takes API"}

@app.get("/users/{username}/ratings")
async def get_user_ratings(username, force_sync = False):
    movies = await get_user_ratings_or_sync(username, force=force_sync)
    if not movies:
        return {"error": f"Username '{username}' not found or has no rated movies."}

    hotness_sorted = calculate_hotness(movies)
    return {"username": username, "movies": hotness_sorted}


@app.get("/users/{username}/recommendations")
async def get_recs(username, k= 20, min_votes = 0):
    await load_catalog(collection)
    doc = await user_ratings_collection.find_one(
        {"lb_username": username}, {"_id": 0, "ratings": 1}
    )
    if not doc or not doc.get("ratings"):
        return {"error": f"No stored ratings for '{username}'"}
    recs = recommend_from_ratings(doc["ratings"], k=k, min_votes=min_votes)
    return {"username": username, "k": k, "min_votes": min_votes, "count": len(recs), "recommendations": recs}

@app.get("/test-catalog")
async def test_catalog():
    await load_catalog(collection)