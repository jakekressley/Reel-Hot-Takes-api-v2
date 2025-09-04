from fastapi import FastAPI
from scraper import scrape_user
from scoring import calculate_hotness

app = FastAPI(title="Reel Hot Takes API")

@app.get("/")
def root():
    return {"message": "Welcome to the Reel Hot Takes API"}

@app.get("/user_ratings/{username}")
async def get_user_ratings(username: str):
    movies = await scrape_user(username)
    if movies is None or len(movies) == 0:
        return {"error": f"Username '{username}' not found or has no rated movies."}
    hotness_sorted = calculate_hotness(movies)
    return {"username": username, "movies": hotness_sorted}


# TODO add more endpoints