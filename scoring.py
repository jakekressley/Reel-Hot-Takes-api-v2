import math

def calculate_hotness(scores):
    filtered = []
    for movie in scores:
        imdb_avg = movie.get('average', 0)
        user_score = movie.get('user_rating', 0)
        votes = movie.get('votes', 0)
        if isinstance(votes, dict):
            votes = 0
        num_votes = max(votes, 0)

        # 1. Core distance scaled to 100, with asymmetry
        if isinstance(imdb_avg, dict):
            imdb_avg = 0

        # Make distance from average much more important, reduce difficulty factor
        max_possible_distance = (9.3 - 1) * 6  # For a 1 on 9.3 (shawshank)
        if user_score < imdb_avg:
            # Rating lower than average is higher hotness
            distance = abs(user_score - imdb_avg) * 6
        else:
            # Rating higher than average less hot
            distance = abs(user_score - imdb_avg) * 2.5
        # Scale so that only the most extreme case yields 110
        distance_scaled = min((distance / max_possible_distance) * 110, 110)

        # Votes very slight tiebreaker with more votes having slightly hotter take
        votes_component = 1 + 0.05 * math.log10(1 + num_votes / 200000)

        hotness = distance_scaled * votes_component

        hotness_value = round(hotness, 2)
        movie_items = list(movie.items())
        movie = dict([('hotness', hotness_value)] + movie_items)
        filtered.append(movie)

    return sorted(filtered, key=lambda x: x['hotness'], reverse=True)