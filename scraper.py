import asyncio
import aiohttp
from bs4 import BeautifulSoup
from db import collection
import os

BASE_URL = "https://letterboxd.com/{}/films/page/{}/"
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
sem = asyncio.Semaphore(10)


async def fetch(session, url):
    async with sem:
        async with session.get(url) as resp:
            return await resp.text()


async def get_page_count(username):
    url = f"https://letterboxd.com/{username}/films/"
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, url)
        soup = BeautifulSoup(html, "lxml")
        try:
            page_data = soup.find_all("li", class_="paginate-page")[-1]
            return int(page_data.find("a").text.replace(",", ""))
        except IndexError:
            return 1
        except Exception:
            print(f"[Warning] Could not fetch pages for user '{username}'")
            return 0

"""
Get the Letterboxd film pages for a given user
"""
async def fetch_letterboxd_pages(username, total_pages):
    movies_dict = {}
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, BASE_URL.format(username, p)) for p in range(1, total_pages + 1)]
        pages = await asyncio.gather(*tasks)

        for html in pages:
            soup = BeautifulSoup(html, "lxml")
            results = soup.find(class_="grid")
            
            if not results:
                continue
            for movie in results.find_all("li", class_="griditem"):
                try:
                    title = movie.find("img")['alt']
                    try:
                        rating_class = movie.find("span", class_="rating")['class'][-1]
                        user_rating = int(rating_class.split("-")[-1])
                    except Exception as e:
                        continue
                    parent_div = movie.find("div", class_="react-component")
                    if not parent_div:
                        print("[Warning] Skipping due to missing parent div")
                        continue

                    movie_link = "https://letterboxd.com" + parent_div["data-item-link"]

                    if user_rating == 0 or title in movies_dict:
                        continue
                    movies_dict[title] = {
                        "title": title,
                        "link": movie_link,
                        "user_rating": user_rating,
                        "average": 0,
                        "votes": 0,
                        "genres": [],
                        "overview": "",
                        "poster": "",
                        "year": ""
                    }
                except Exception as e:
                    print(f"[Warning] Skipping a movie due to parse error: {e} at {title}")
                    continue
            
    return list(movies_dict.values())

"""
Gets the corresponding movie data from TMDB for things like vote count and a more accurate average
"""
async def fetch_tmdb_data(session, movie_title, letterboxd_url):
    # Try Mongo before scraping
    existing = await collection.find_one({"Title": movie_title})
    #print(existing)
    if existing:
        return {
            "Average Score": existing.get("Average Score", 5),
            "Vote Count": existing.get("Vote Count", 1000),
            "Genres": existing.get("Genres", []),
            "Overview": existing.get("Overview", ""),
            "Poster": existing.get("Poster", ""),
            "Year": existing.get("Year", "")
        }

    # if not in mongo get tmdb link via scraping letterboxd
    try:
        #print("LETTERBOXD_URL", letterboxd_url)
        html = await fetch(session, letterboxd_url)
        soup = BeautifulSoup(html, "lxml")
        tmdb_tag = soup.find("p", class_="text-link text-footer")
        tmdb_link = tmdb_tag.find("a", attrs={"data-track-action": "TMDB"})["href"]
        #print(tmdb_link)

        if not tmdb_link:
            return {}

        tmdb_id = tmdb_link.split('/')[4]
        #print(tmdb_id)
        api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}"
        async with session.get(api_url) as resp:
            data = await resp.json()
            movie_data = {
                "tmdb_id": tmdb_id,
                "Average Score": data.get("vote_average", 0),
                "Genres": [g["name"] for g in data.get("genres", [])],
                "Overview": data.get("overview", ""),
                "Poster": data.get("poster_path", ""),
                "Vote Count": data.get("vote_count", 0),
                "Year": data.get("release_date", "")[:4]
            }
            #print(movie_data)
            # Store in Mongo for caching faster use next time
            collection.update_one(
                {"Title": data.get("title", movie_title)},
                {"$set": {**movie_data, "tmdb_id": data.get("id", tmdb_id), "Title": data.get("title", movie_title)}},
                upsert=True
            )
            return movie_data
    except Exception as e:
        print(f"[Warning] TMDB fetch failed for {movie_title}: {e}")
        return {}


async def update_movies_with_tmdb(movies):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_tmdb_data(session, m['title'], m['link']) for m in movies]
        results = await asyncio.gather(*tasks)
        for movie, tmdb_data in zip(movies, results):
            movie["average"] = tmdb_data.get("Average Score", 0)
            movie["votes"] = tmdb_data.get("Vote Count", 0)
            movie["genres"] = tmdb_data.get("Genres", [])
            movie["overview"] = tmdb_data.get("Overview", "")
            movie["poster"] = tmdb_data.get("Poster", "")
            movie["year"] = tmdb_data.get("Year", "")
    return movies


async def scrape_user(username):
    total_pages = await get_page_count(username)
    if total_pages == 0:
        return []
    movies = await fetch_letterboxd_pages(username, total_pages)
    movies = await update_movies_with_tmdb(movies)
    return movies
