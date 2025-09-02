from fastapi import FastAPI
from scraper import scrape_user
from scoring import calculate_hotness

app = FastAPI(title="CineScout API")

@app.get("/")
def root():
    return {"message": "Welcome to CineScout API"}

@app.get("/user_ratings/{username}")
async def get_user_ratings(username: str):
    movies = await scrape_user(username)
    hotness_sorted = calculate_hotness(movies)
    return {"username": username, "movies": hotness_sorted}

# TODO add more endpoints
# TODO add logic for if letterboxd username is not found