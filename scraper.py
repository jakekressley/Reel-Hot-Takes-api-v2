import asyncio
import aiohttp
from lxml import etree
from lxml.cssselect import CSSSelector
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

                    }
                except Exception as e:
                    print(f"[Warning] Skipping a movie due to parse error: {e} at {title}")
                    continue
            
    return list(movies_dict.values())

async def fetch_imdb_data(session, movie_title, imdb_url):
    async with session.get(imdb_url) as resp:
        try:
            html = await resp.text()
            soup = BeautifulSoup(html, "lxml")

            average = 0
            votes = 0
            rating_tag = soup.find("div", class_="ipxRZe")
            print(rating_tag)
            poster = soup.find("div", class_="ipc-media")
            print(imdb_url, poster)

            return average, votes, poster
        except Exception as e:
            print(f"[Warning] IMDB scrape failed for {movie_title}: {e}")
            return None


async def fetch_letterboxd_data(session, movie_title, letterboxd_url):
    """
    Scrape a film page on Letterboxd for average, votes, genres, overview, poster, director, year.
    Falls back to Mongo cache if available.
    """
    # 1. Check Mongo first
    existing = await collection.find_one({"title": movie_title})
    if existing:
        return {
            "tagline" : existing.get("tagline", ""),
            "director": existing.get("director", ""),
            "genres": existing.get("genres", []),
            "overview": existing.get("overview", ""),
            "poster": existing.get("poster", ""),
            "year": existing.get("year", ""),
            "average": existing.get("average"),
            "votes": existing.get("votes"),
        }

    # 2. Scrape from Letterboxd
    try:
        html = await fetch(session, letterboxd_url)
        soup = BeautifulSoup(html, "lxml")

        production_info = soup.find("div", class_="productioninfo")
        year = production_info.find("span", class_="releasedate").find("a").text.strip()
        director_tags = production_info.find("span", class_="creatorlist").find_all("a")
        # logic for multiple directors
        directors = [tag.text.strip() for tag in director_tags][:2]
        director = ", ".join(directors)

        production_synopsis = soup.find("section", class_="production-synopsis")
        tagline = production_synopsis.find("h4", class_="tagline").text.strip()
        overview = production_synopsis.find("div", class_="truncate").text.strip()
        
        imdb_tag = soup.find("p", class_="text-link text-footer")
        imdb_url = imdb_tag.find("a", attrs={"data-track-action": "IMDb"})["href"]
        imdb_url = imdb_url.rsplit('/', 1)[0]
        

        average, votes, poster = await fetch_imdb_data(session, movie_title, imdb_url)
        #print(poster)
        genres = [],


        movie_data = {
            "year": year,
            "director": director,
            "tagline" : tagline,
            "overview": overview,
            "poster": poster,
            "genres": genres,
            "average": average,
            "votes": votes,
        }

        """
        await collection.update_one(
            {"title": movie_title},
            {"$set": {**movie_data, "title": movie_title}},
            upsert=True
        )
        """

        return movie_data

    except Exception as e:
        print(f"[Warning] Letterboxd scrape failed for {movie_title}: {e}")
        return {}

async def update_movies_with_letterboxd(movies):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_letterboxd_data(session, m['title'], m['link']) for m in movies]
        results = await asyncio.gather(*tasks)

        for movie, lb_data in zip(movies, results):
            movie["average"] = lb_data.get("average", 0)
            movie["votes"] = lb_data.get("votes", 0)
            movie["genres"] = lb_data.get("genres", [])
            movie["overview"] = lb_data.get("overview", "")
            movie["poster"] = lb_data.get("poster", "")
            movie["director"] = lb_data.get("director", "")
            movie["year"] = lb_data.get("year", "")

    return movies

async def scrape_user(username):
    total_pages = await get_page_count(username)
    if total_pages == 0:
        print(f"[Error] Invalid or non-existent Letterboxd username: {username}")
        return []
    movies = await fetch_letterboxd_pages(username, total_pages)
    movies = await update_movies_with_letterboxd(movies)
    return movies
