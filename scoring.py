import math

# TODO: refactor calculation maybe stop using tmdb
def calculate_hotness(scores):
    filtered = []
    for movie in scores:
        avg_rating = movie['average']
        user_score = movie['user_rating']
        num_votes = movie['votes']

        weighted_votes = round((3 * math.log10(1 + num_votes / 5000) + 7), 2)
        weighted_average = round(3.57 * avg_rating - 17.68, 2)
        weighted_distance = abs(user_score - weighted_average)
        hotness = weighted_distance * 6 + weighted_votes * 3
        movie['hotness'] = round(hotness, 2)
        if movie['hotness'] <= 100:
            filtered.append(movie)
    return sorted(filtered, key=lambda x: x['hotness'], reverse=True)
