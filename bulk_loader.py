import asyncio
import aiohttp
import csv
from db import collection  # make sure db.py is accessible here

API_URL = "https://api.imdbapi.dev/titles/{}"

async def fetch_imdb_movie(session, imdb_id):
    existing = await collection.find_one({"imdb_id": imdb_id})
    if existing:
        print(f"[Skipping] {existing.get('title', 'No Title')} ({imdb_id}) already in DB")
        return None

    async with session.get(API_URL.format(imdb_id)) as resp:
        if resp.status != 200:
            print(f"[Error] {imdb_id} failed with status {resp.status}")
            return None
        data = await resp.json()
        return data  # Use the full JSON object

async def preload_movies_from_csv(csv_file):
    imdb_ids = []

    # 1. Read CSV and pull imdbId column
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header if present
        for row in reader:
            if len(row) >= 2 and row[1].strip():  # ensure imdbId exists
                imdb_ids.append("tt" + row[1].strip().zfill(7))  # pad with zeros if needed

    print(f"Found {len(imdb_ids)} IMDb IDs in {csv_file}")

    # 2. Process sequentially with .1 second delay
    async with aiohttp.ClientSession() as session:
        for imdb_id in imdb_ids:
            data = await fetch_imdb_movie(session, imdb_id)
            if data:
                movie_doc = {
                    "imdb_id": imdb_id,
                    "type": data.get("type", ""),
                    "title": data.get("primaryTitle", ""),
                    "poster": data.get("primaryImage", {}).get("url"),
                    "year": data.get("startYear"),
                    "runtimeSeconds": data.get("runtimeSeconds"),
                    "genres": data.get("genres", []),
                    "average": data.get("rating", {}).get("aggregateRating"),
                    "votes": data.get("rating", {}).get("voteCount"),
                    "directors": data.get("directors", []),
                    "writers": data.get("writers", []),
                    "stars": data.get("stars", []),
                    "originCountries": data.get("originCountries", []),
                    "spokenLanguages": data.get("spokenLanguages", []),
                    "interests": data.get("interests", []),
                }
                await collection.update_one(
                    {"imdb_id": movie_doc["imdb_id"]},
                    {"$set": movie_doc},
                    upsert=True
                )
                print(f"[Inserting] {movie_doc['title']} ({imdb_id})")

            # Sleep 1 second before the next request
            await asyncio.sleep(0.05)

# Allow running as standalone script
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python bulk_loader.py <csv_file>")
    else:
        asyncio.run(preload_movies_from_csv(sys.argv[1]))
