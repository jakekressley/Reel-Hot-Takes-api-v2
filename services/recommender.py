# recommender_mongo.py
from __future__ import annotations
import re
import difflib
from typing import Dict, Any, List, Optional

import numpy as np
from motor.motor_asyncio import AsyncIOMotorCollection
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from scipy.sparse import hstack, csr_matrix

# ----------------- In-memory catalog -----------------
_catalog_loaded = False
_rows: List[Dict[str, Any]] = []              # catalog rows in order
_title2idx: Dict[str, int] = {}               # title -> row index
_titleyear2idx: Dict[str, int] = {}           # normalized "title::year" -> row index

_vectorizer: Optional[TfidfVectorizer] = None
_features = None                               # sparse (N, D_text + D_num)

# ----------------- helpers -----------------
_punct_re = re.compile(r"[^\w\s]")

def _safe_join(parts):
    return " ".join(p.strip() for p in parts if isinstance(p, str) and p.strip())

def _norm_title(t: str) -> str:
    if not t:
        return ""
    t = t.lower().strip()
    if t.endswith(", the"): t = "the " + t[:-5].strip()
    elif t.endswith(", a"): t = "a " + t[:-3].strip()
    elif t.endswith(", an"): t = "an " + t[:-4].strip()
    t = _punct_re.sub("", t)
    return " ".join(t.split())

def _mk_key(title: str, year: Any | None) -> str:
    title_key = _norm_title(title)
    y = str(year).strip() if year is not None else ""
    return f"{title_key}::{y}" if y else title_key

def _feature_text(doc: dict) -> str:
    # Accept both Title/title, etc.
    title    = str(doc.get("Title") or doc.get("title") or "")
    overview = str(doc.get("Overview") or doc.get("overview") or "")
    genres_v = doc.get("Genres") or doc.get("genres") or []
    genres   = " ".join(genres_v) if isinstance(genres_v, list) else str(genres_v or "")
    avg      = str(doc.get("Average Score") or doc.get("average") or doc.get("vote_average") or "")
    votes    = str(doc.get("Vote Count")    or doc.get("votes")   or doc.get("vote_count")    or "")
    year     = str(doc.get("Year") or doc.get("year") or "")
    text = _safe_join([title, genres, overview, avg, votes, year])
    return text if text else "unknown"

def _get_numeric(doc: dict):
    # Three simple numeric signals; robust to missing values
    def to_float(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default
    avg   = to_float(doc.get("Average Score") or doc.get("average") or doc.get("vote_average"))
    votes = to_float(doc.get("Vote Count")    or doc.get("votes")   or doc.get("vote_count"))
    year  = to_float(doc.get("Year")          or doc.get("year"))
    return avg, votes, year

# ----------------- catalog load -----------------
async def load_catalog(collection: AsyncIOMotorCollection, limit: int | None = None) -> None:
    """
    Load catalog from Mongo and build a hybrid feature matrix:
      [ TF-IDF(Title + Genres + Overview + nums-as-tokens) | scaled numeric columns (avg, votes, year) ]
    """
    global _catalog_loaded, _rows, _title2idx, _titleyear2idx, _vectorizer, _features

    projection = {
        "_id": 0,
        # include both cases to be safe
        "Title": 1, "title": 1,
        "Genres": 1, "genres": 1,
        "Overview": 1, "overview": 1,
        "Average Score": 1, "average": 1, "vote_average": 1,
        "Vote Count": 1, "votes": 1, "vote_count": 1,
        "Year": 1, "year": 1,
        "Poster": 1, "poster": 1,
    }

    # Fetch
    cursor = collection.find({"votes": {"$gt": 100_000}}, projection=projection)
    if limit:
        cursor = cursor.limit(limit)
    docs = await cursor.to_list(length=None)
    # print(docs)

    if not docs:
        raise RuntimeError("Catalog query returned 0 documents. Verify DB, collection, and projection.")

    # Store rows + build title indices
    _rows = []
    _title2idx = {}
    _titleyear2idx = {}
    for i, d in enumerate(docs):
        _rows.append(d)
        title = (d.get("Title") or d.get("title") or "").strip()
        year  = d.get("Year") or d.get("year")
        if title:
            _title2idx[title.lower()] = i
            _title2idx.setdefault(_norm_title(title), i)
            _titleyear2idx[_mk_key(title, year)] = i

    # --- Text features
    corpus = [_feature_text(d) for d in _rows]
    _vectorizer = TfidfVectorizer(
        lowercase=True,
        token_pattern=r"(?u)\b\w+\b",  # keep numbers & 1-char tokens
        min_df=1,
        stop_words=None,
        ngram_range=(1, 2),           # optional: unigrams+bigrams
    )
    X_text = _vectorizer.fit_transform(corpus)

    # --- Numeric features (scaled) and hstack with text
    num = np.array([_get_numeric(d) for d in _rows], dtype=np.float32)  # shape (N,3)
    if num.size == 0:
        X_num = csr_matrix((len(_rows), 0))
    else:
        scaler = StandardScaler(with_mean=False)  # with_mean=False for sparse compatibility
        # We’ll scale dense then convert to sparse; keep it simple & small (#cols=3)
        num_scaled = StandardScaler().fit_transform(num)
        X_num = csr_matrix(num_scaled)

    _features = hstack([X_text, X_num], format="csr")
    _catalog_loaded = True

# ----------------- scoring utils -----------------
def _normalize_weights(user_movies: List[Dict[str, Any]]) -> List[float]:
    """
    Convert user ratings to weights in [-1,1], auto-detecting 0–5 or 0–10 scale.
    """
    ratings = [m.get("user_rating") for m in user_movies if m.get("user_rating") is not None]
    if not ratings:
        return [0.0 for _ in user_movies]
    max_r = max(ratings)
    scale_max = 5.0 if max_r <= 5.0 else 10.0
    mid = scale_max / 2.0
    return [((m.get("user_rating", mid)) - mid) / mid for m in user_movies]

def _map_movie_to_row(movie: Dict[str, Any]) -> Optional[int]:
    """
    Title-based mapping only (no external IDs).
    Try title+year key -> exact lower -> normalized -> fuzzy.
    """
    title = (movie.get("title") or movie.get("Title") or "").strip()
    if not title:
        return None
    year  = movie.get("year") or movie.get("Year")

    # 1) title+year
    k_ty = _mk_key(title, year)
    if k_ty in _titleyear2idx:
        return _titleyear2idx[k_ty]

    # 2) title-only
    key = title.lower()
    if key in _title2idx:
        return _title2idx[key]
    nkey = _norm_title(title)
    if nkey in _title2idx:
        return _title2idx[nkey]

    # 3) fuzzy
    if _title2idx:
        close = difflib.get_close_matches(nkey or key, list(_title2idx.keys()), n=1, cutoff=0.82)
        if close:
            return _title2idx[close[0]]
    return None

def _build_user_profile(rated_rows: List[int], weights: List[float]):
    """
    Weighted sum of feature rows -> L2-normalized user profile vector.
    """
    if not rated_rows:
        return None
    rows = _features[rated_rows]  # (R,D) sparse
    w = np.asarray(weights, dtype=np.float32).reshape(-1, 1)
    prof = rows.T @ w             # (D,1)
    prof = prof.ravel()
    n = np.linalg.norm(prof)
    return (prof / n) if n > 0 else prof

# ----------------- public API -----------------
def recommend_from_ratings(user_movies: List[Dict[str, Any]], k: int = 10, min_votes: int = 0) -> List[Dict[str, Any]]:
    if not _catalog_loaded:
        raise RuntimeError("Catalog not loaded; call load_catalog() first.")

    rated_rows: List[int] = []
    matched: List[Dict[str, Any]] = []
    for m in user_movies:
        r = _map_movie_to_row(m)
        if r is not None:
            rated_rows.append(r)
            matched.append(m)

    if not rated_rows:
        # Cold-start fallback: top-K by votes
        ranked = sorted(
            range(len(_rows)),
            key=lambda i: (int((_rows[i].get("Vote Count") or _rows[i].get("votes") or 0))),
            reverse=True
        )[:k]
        return [{
            "title": _rows[i].get("Title") or _rows[i].get("title"),
            "poster": _rows[i].get("Poster") or _rows[i].get("poster"),
            "year": _rows[i].get("Year") or _rows[i].get("year"),
            "genres": _rows[i].get("Genres") or _rows[i].get("genres", []),
            "score": 0.0,
            "average": _rows[i].get("Average Score") or _rows[i].get("average"),
            "votes": _rows[i].get("Vote Count") or _rows[i].get("votes"),
        } for i in ranked]

    weights = _normalize_weights(matched)
    user_vec = _build_user_profile(rated_rows, weights)
    if user_vec is None or user_vec.shape[0] == 0:
        return []

    # Cosine similarity via sparse matvec
    scores = (_features @ user_vec).astype(np.float32).ravel()

    # Blend a touch of popularity to avoid ultra-obscure ties (optional)
    pop = np.array([int((r.get("Vote Count") or r.get("votes") or 0)) for r in _rows], dtype=np.float32)
    pop = np.tanh(pop / 10000.0)
    scores = 0.9 * scores + 0.1 * pop

    # Exclude seen
    seen = set(rated_rows)
    scores[list(seen)] = -np.inf

    # Optional popularity filter
    if min_votes > 0:
        for i, d in enumerate(_rows):
            vc = d.get("Vote Count") or d.get("votes") or 0
            try:
                vc = int(vc)
            except Exception:
                vc = 0
            if vc < min_votes:
                scores[i] = -np.inf

    # Top-K
    valid = np.isfinite(scores)
    if not np.any(valid):
        return []
    k = min(k, int(valid.sum()))
    top_idx = np.argpartition(-scores, kth=k-1)[:k]
    top_idx = top_idx[np.argsort(-scores[top_idx])]

    recs: List[Dict[str, Any]] = []
    for i in top_idx:
        if not np.isfinite(scores[i]):
            continue
        d = _rows[i]
        recs.append({
            "title":  d.get("Title")  or d.get("title"),
            "poster": d.get("Poster") or d.get("poster"),
            "year":   d.get("Year")   or d.get("year"),
            "genres": d.get("Genres") or d.get("genres", []),
            "score":  float(scores[i]),
            "average": d.get("Average Score") or d.get("average"),
            "votes":   d.get("Vote Count")    or d.get("votes"),
        })
    return recs
