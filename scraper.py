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
        # Check for invalid username
        not_found = soup.find("body", class_="error")
        if not_found:
            print(f"[Warning] Username '{username}' not found on Letterboxd.")
            return 0
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
                    #poster_link = movie.find("div", class_="film-poster").find("img", class_="image")['src']
                    #print(poster_link)

                    if user_rating == 0 or title in movies_dict:
                        continue
                    movies_dict[title] = {
                        "title": title,
                        "link": movie_link,
                        "user_rating": user_rating,
                        "imdb_id": "",
                    }
                except Exception as e:
                    print(f"[Warning] Skipping a movie due to parse error: {e} at {title}")
                    continue
            
    return movies_dict

async def fetch_imdb_data(session, movie_title, imdb_url):
    async with session.get(imdb_url) as resp:
        try:
            html = await resp.text()
            soup = BeautifulSoup(html, "lxml")

            average = 0
            votes = 0
            rating_tag = soup.find("div", class_="ipxRZe")
            # print(rating_tag)
            poster = soup.find("div", class_="ipc-media")
            # print(imdb_url, poster)

            return average, votes, poster
        except Exception as e:
            print(f"[Warning] IMDB scrape failed for {movie_title}: {e}")
            return None


async def fetch_letterboxd_data(session, movie_title, letterboxd_url):
    """
    Get a films data (title, year, etc) via database, if not in database then add to it
    """
    # check if in mongo database
    existing = await collection.find_one({"title": movie_title})
    if existing:
        return {
            "imdb_id": existing.get("imdb_id", ""),
            "type": existing.get("type", ""),
            "title": existing.get("title", ""),
            "poster": existing.get("poster", {}),
            "year": existing.get("year"),
            "runtimeSeconds": existing.get("runtimeSeconds"),
            "genres": existing.get("genres", []),
            "average": existing.get("average", 0),
            "votes": existing.get("votes", 0),
            "directors": existing.get("directors", []),
            "writers": existing.get("writers", []),
            "stars": existing.get("stars", []),
            "originCountries": existing.get("originCountries", []),
            "spokenLanguages": existing.get("spokenLanguages", []),
            "interests": existing.get("interests", []),
        }

    # Get from IMDB API if not in database
    else:
        try:
            html = await fetch(session, letterboxd_url)
            soup = BeautifulSoup(html, "lxml")
            imdb_tag = soup.find("p", class_="text-link text-footer")
            imdb_link = imdb_tag.find("a", attrs={"data-track-action": "IMDb"})["href"]
            imdb_id = imdb_link.rstrip('/').split('/')[-2]

            imdb_api_url = f"https://api.imdbapi.dev/titles/{imdb_id}"
            async with session.get(imdb_api_url) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                else:
                    text = await resp.text()
                    print(f"IMDB API error: status={resp.status}, body={text}")
                    return {}

            # print(f"title: {movie_title}\nyear: {year}\ndirector: {director}\noverview: {overview}\naverage: {average}\nvotes: {votes}\nposter: {poster}\ngenres: {genres}\n")

            movie_data = {
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
                {"title": movie_title},
                {"$set": {**movie_data, "title": movie_title}},
                upsert=True
            )

            await asyncio.sleep(0.15)

            return movie_data

        except Exception as e:
            print(f"[Warning] Letterboxd scrape failed for {movie_title}: {e}")
            return {}

async def update_movies_with_letterboxd(movies, movies_dict):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_letterboxd_data(session, m['title'], m['link']) for m in movies]
        results = await asyncio.gather(*tasks)

        for movie, lb_data in zip(movies, results):
            movie["imdb_id"] = lb_data.get("imdb_id", "")
            movie["type"] = lb_data.get("type", "")
            movie["title"] = lb_data.get("title", "")
            movie["poster"] = lb_data.get("poster", "")
            movie["year"] = lb_data.get("year", "")
            movie["runtimeSeconds"] = lb_data.get("runtimeSeconds", None)
            movie["genres"] = lb_data.get("genres", [])
            movie["average"] = lb_data.get("average", {})
            movie["votes"] = lb_data.get("votes", {})
            movie["directors"] = lb_data.get("directors", [])
            movie["writers"] = lb_data.get("writers", [])
            movie["stars"] = lb_data.get("stars", [])
            movie["originCountries"] = lb_data.get("originCountries", [])
            movie["spokenLanguages"] = lb_data.get("spokenLanguages", [])
            movie["interests"] = lb_data.get("interests", [])
            movie["overview"] = lb_data.get("overview", "")

    return movies

async def scrape_user(username):
    total_pages = await get_page_count(username)
    if total_pages == 0:
        print(f"[Error] Invalid or non-existent Letterboxd username: {username}")
        return []
    movies_dict = await fetch_letterboxd_pages(username, total_pages)
    movies = list(movies_dict.values())
    movies = await update_movies_with_letterboxd(movies, movies_dict)
    return movies
