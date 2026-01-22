from flask import Flask, render_template, request, redirect, url_for, flash
import pymysql
from pymongo import MongoClient, DESCENDING
from datetime import datetime
import os

exec(open('config.py').read())

app = Flask(__name__)
app.secret_key = SECRET_KEY
mongo_collection = None


def get_mysql_connection():
    try:
        return pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DB,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        print(f"❌ MySQL error: {e}")
        return None


def get_all_genres():
    conn = get_mysql_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT category_id, name FROM category ORDER BY name")
            result = cursor.fetchall()
            conn.close()
            return result
    except Exception:
        conn.close()
        return []


def get_year_range():
    conn = get_mysql_connection()
    if not conn:
        return 1900, 2025
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MIN(release_year) as min_year, MAX(release_year) as max_year FROM film")
            result = cursor.fetchone()
            conn.close()
            min_year = result['min_year'] or 1900
            max_year = result['max_year'] or 2025
            return min_year, max_year
    except Exception:
        conn.close()
        return 1900, 2025


def search_by_keyword(keyword, page=1):
    conn = get_mysql_connection()
    if not conn:
        return [], 0
    try:
        with conn.cursor() as cursor:
            limit = RESULTS_PER_PAGE
            offset = (page - 1) * limit
            query = """
                SELECT 
                    film_id,
                    title,
                    description,
                    release_year,
                    rating
                FROM film
                WHERE title LIKE %s
                ORDER BY title
                LIMIT %s OFFSET %s
            """
            search_pattern = f"%{keyword}%"
            cursor.execute(query, (search_pattern, limit, offset))
            movies = cursor.fetchall()
            cursor.execute("SELECT COUNT(*) as total FROM film WHERE title LIKE %s", (search_pattern,))
            total = cursor.fetchone()['total']
            conn.close()
            return movies, total
    except Exception as e:
        print(f"❌ Search error: {e}")
        conn.close()
        return [], 0


def search_by_genre_and_year(genre_id, start_year, end_year, page=1):
    conn = get_mysql_connection()
    if not conn:
        return [], 0
    try:
        with conn.cursor() as cursor:
            limit = RESULTS_PER_PAGE
            offset = (page - 1) * limit
            query = """
                SELECT 
                    f.film_id,
                    f.title,
                    f.description,
                    f.release_year,
                    f.rating
                FROM film f
                JOIN film_category fc ON f.film_id = fc.film_id
                WHERE fc.category_id = %s
                AND f.release_year BETWEEN %s AND %s
                ORDER BY f.title
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (genre_id, start_year, end_year, limit, offset))
            movies = cursor.fetchall()
            count_query = """
                SELECT COUNT(*) as total 
                FROM film f 
                JOIN film_category fc ON f.film_id = fc.film_id
                WHERE fc.category_id = %s 
                AND f.release_year BETWEEN %s AND %s
            """
            cursor.execute(count_query, (genre_id, start_year, end_year))
            total = cursor.fetchone()['total']
            conn.close()
            return movies, total
    except Exception as e:
        print(f"❌ Search error: {e}")
        conn.close()
        return [], 0


def get_movie_details(film_id):
    """Получаем детали фильма (даже если поля пустые)"""
    conn = get_mysql_connection()
    if conn is None:
        return None

    try:
        with conn.cursor() as cursor:
            query = """
                SELECT 
                    film_id,
                    title,
                    description,
                    release_year,
                    rating,
                    length,
                    rental_duration,
                    rental_rate,
                    replacement_cost
                FROM film
                WHERE film_id = %s
            """
            cursor.execute(query, (film_id,))
            movie = cursor.fetchone()
            conn.close()
            return movie
    except Exception:
        conn.close()
        return None


def init_mongo():
    global mongo_collection
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        mongo_collection = db[MONGO_COLLECTION]
        mongo_collection.create_index([("timestamp", DESCENDING)])
        print("✓ MongoDB connected")
        return True
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return False


def log_search(search_type, params, results_count):
    if mongo_collection is None:
        return
    try:
        log_doc = {
            "timestamp": datetime.now(),
            "search_type": search_type,
            "params": params,
            "results_count": results_count
        }
        mongo_collection.insert_one(log_doc)
    except Exception:
        pass


def get_statistics():
    if mongo_collection is None:
        return {"popular": [], "recent": []}
    try:
        popular = list(mongo_collection.aggregate([
            {"$group": {"_id": {"search_type": "$search_type", "params": "$params"},
                        "count": {"$sum": 1},
                        "last_searched": {"$max": "$timestamp"}}},
            {"$sort": {"count": DESCENDING}},
            {"$limit": STATS_TOP_QUERIES}
        ]))
        recent = list(mongo_collection.aggregate([
            {"$sort": {"timestamp": DESCENDING}},
            {"$group": {"_id": {"search_type": "$search_type", "params": "$params"},
                        "last_searched": {"$max": "$timestamp"}}},
            {"$sort": {"last_searched": DESCENDING}},
            {"$limit": STATS_TOP_QUERIES}
        ]))
        return {"popular": popular, "recent": recent}
    except Exception:
        return {"popular": [], "recent": []}


@app.route('/')
def index():
    genres = get_all_genres()
    min_year, max_year = get_year_range()
    stats = get_statistics()
    return render_template('index.html', genres=genres, min_year=min_year, max_year=max_year, stats=stats)


@app.route('/search')
def search():
    search_type = request.args.get('type', 'keyword')
    page = int(request.args.get('page', 1))

    if search_type == 'keyword':
        keyword = request.args.get('keyword', '').strip()
        if not keyword:
            flash('Введите ключевое слово', 'warning')
            return redirect(url_for('index'))

        movies, total = search_by_keyword(keyword, page)
        log_search("keyword", {"keyword": keyword}, len(movies))

        summary = {
            "type": "keyword",
            "description": f"Search: '{keyword}'",
            "page": page,
            "total": total,
            "has_prev": page > 1,
            "has_next": (page * 10) < total
        }
        return render_template('search_results.html', movies=movies, summary=summary, keyword=keyword)

    else:
        genre_id = int(request.args.get('genre_id'))
        start_year = int(request.args.get('start_year'))
        end_year = int(request.args.get('end_year'))

        movies, total = search_by_genre_and_year(genre_id, start_year, end_year, page)
        log_search("genre_year", {
            "genre_id": genre_id,
            "start_year": start_year,
            "end_year": end_year
        }, len(movies))

        summary = {
            "type": "genre_year",
            "description": f"Genre ID:{genre_id}, Years: {start_year}-{end_year}",
            "page": page,
            "total": total,
            "has_prev": page > 1,
            "has_next": (page * 10) < total,
            "genre_id": genre_id,
            "start_year": start_year,
            "end_year": end_year
        }

        return render_template('search_results.html', movies=movies, summary=summary, keyword=None)


@app.route('/movie/<int:film_id>')
def movie_detail(film_id):
    movie = get_movie_details(film_id)
    if not movie:
        flash('Film not found', 'error')
        return redirect(url_for('index'))
    return render_template('movie.html', movie=movie)


@app.route('/stats')
def stats_page():
    stats = get_statistics()
    return render_template('stats.html', stats=stats)


@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500


if __name__ == '__main__':
    if not init_mongo():
        print("Failed to connect to MongoDB!")
        exit(1)
    print("✓ Everything ready! Open http://localhost:5000")
    app.run(debug=False, host='0.0.0.0', port=5000)