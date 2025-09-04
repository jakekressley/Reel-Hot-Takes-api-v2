import asyncio
import aiohttp
import csv
from db import collection

API_URL = "https://api.imdbapi.dev/titles/{}"

errorCount = 0
erroredMovies = set()

async def fetch_imdb_movie(session, imdb_id):
    existing = await collection.find_one({"imdb_id": imdb_id})
    if existing:
        print(f"[Skipping] {existing.get('title', 'No Title')} ({imdb_id}) already in DB")
        return None

    async with session.get(API_URL.format(imdb_id)) as resp:
        if resp.status != 200:
            print(f"[Error] {imdb_id} failed with status {resp.status}")
            return "ERROR"
        data = await resp.json()
        return data

async def preload_movies_from_csv(csv_file):
    imdb_ids = []
    errored_ids = []

    # 1. Read CSV and pull imdbId column
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  
        for row in reader:
            if len(row) >= 2 and row[1].strip(): 
                # 0 padding if necessary
                imdb_ids.append("tt" + row[1].strip().zfill(7)) 

    print(f"Found {len(imdb_ids)} IMDb IDs in {csv_file}")

    # Load into Database
    async with aiohttp.ClientSession() as session:
        for imdb_id in imdb_ids:
            data = await fetch_imdb_movie(session, imdb_id)
            if data == "ERROR":
                errored_ids.append(imdb_id)
            elif data:
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

            # Sleep .3 second before the next request to avoid the 429
            await asyncio.sleep(0.3)

    print(f"Total errored: {len(errored_ids)}")
    print(f"Errored IMDb IDs: {errored_ids}")

    with open("errored_ids.txt", "w") as f:
        for imdb_id in errored_ids:
            f.write(imdb_id + "\n")
        print(f"Saved {len(errored_ids)} errored IDs to errored_ids.txt")

    return errored_ids

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python bulk_loader.py <csv_file>")
    else:
        asyncio.run(preload_movies_from_csv(sys.argv[1]))
