import math

def calculate_hotness(scores):
    SOFT_MAX_RATING = 8.8   # treat this as the effective max IMDb rating
    filtered = []

    for movie in scores:
        imdb_avg = movie.get('average', 0)
        user_score = movie.get('user_rating', 0)
        num_votes = max(movie.get('votes', 0), 0)

        # 1. Core distance scaled to 100
        distance = abs(user_score - imdb_avg)
        distance_scaled = min((distance / SOFT_MAX_RATING) * 100, 100)

        # 2. Votes component — much lighter impact
        # grows slowly with log10(votes), then scaled down
        votes_component = 1 + 0.5 * math.log10(1 + num_votes / 50000)

        # 3. Final hotness = distance scaled × small multiplier
        hotness = distance_scaled * votes_component

        movie['hotness'] = round(hotness, 2)
        filtered.append(movie)

    return sorted(filtered, key=lambda x: x['hotness'], reverse=True)
