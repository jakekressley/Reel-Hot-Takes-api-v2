from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from scraper import scrape_user
from scoring import calculate_hotness

app = FastAPI(title="Reel Hot Takes API")

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173/",
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

@app.get("/user_ratings/{username}")
async def get_user_ratings(username: str):
    movies = await scrape_user(username)
    if movies is None or len(movies) == 0:
        return {"error": f"Username '{username}' not found or has no rated movies."}
    hotness_sorted = calculate_hotness(movies)
    return {"username": username, "movies": hotness_sorted}


# TODO add more endpoints