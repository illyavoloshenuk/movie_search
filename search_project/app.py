from flask import Flask, render_template, request, redirect, url_for, flash
import pymysql
from pymongo import MongoClient, DESCENDING
from datetime import datetime

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


def search_by_keyword(keyword, page=1):
    conn = get_mysql_connection()
    if not conn:
        return [], 0
    limit = 10
    offset = (page - 1) * limit
    try:
        with conn.cursor() as cursor:

            keyword_clean = ' '.join(keyword.split())

            cursor.execute(
                "SELECT film_id, title, release_year, rating FROM film WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s OFFSET %s",
                (f"%{keyword_clean}%", limit, offset))
            movies = cursor.fetchall()
            cursor.execute("SELECT COUNT(*) as total FROM film WHERE LOWER(title) LIKE LOWER(%s)",
                           (f"%{keyword_clean}%",))
            total = cursor.fetchone()['total']
            return movies, total, keyword_clean
    finally:
        conn.close()


def search_by_genre_year(genre_id, start_year, end_year, page=1):
    conn = get_mysql_connection()
    if not conn:
        return [], 0, "All Genres"
    limit = 10
    offset = (page - 1) * limit

    try:
        with conn.cursor() as cursor:
            if genre_id == 0:
                cursor.execute("""
                    SELECT film_id, title, release_year, rating 
                    FROM film 
                    WHERE release_year BETWEEN %s AND %s
                    ORDER BY title LIMIT %s OFFSET %s
                """, (start_year, end_year, limit, offset))
                movies = cursor.fetchall()

                cursor.execute("SELECT COUNT(*) as total FROM film WHERE release_year BETWEEN %s AND %s",
                               (start_year, end_year))
                total = cursor.fetchone()['total']
                return movies, total, "All Genres"
            else:
                cursor.execute("""
                    SELECT f.film_id, f.title, f.release_year, f.rating 
                    FROM film f
                    JOIN film_category fc ON f.film_id = fc.film_id
                    WHERE fc.category_id = %s AND f.release_year BETWEEN %s AND %s
                    ORDER BY f.title LIMIT %s OFFSET %s
                """, (genre_id, start_year, end_year, limit, offset))
                movies = cursor.fetchall()

                cursor.execute("""
                    SELECT COUNT(*) as total FROM film f
                    JOIN film_category fc ON f.film_id = fc.film_id
                    WHERE fc.category_id = %s AND f.release_year BETWEEN %s AND %s
                """, (genre_id, start_year, end_year))
                total = cursor.fetchone()['total']

                cursor.execute("SELECT name FROM category WHERE category_id = %s", (genre_id,))
                genre_name = cursor.fetchone()['name']
                return movies, total, genre_name
    finally:
        conn.close()


def get_all_genres():
    conn = get_mysql_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT category_id, name FROM category ORDER BY name")
            return cursor.fetchall()
    finally:
        conn.close()


def get_year_range():
    conn = get_mysql_connection()
    if not conn:
        return 1900, 2025
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MIN(release_year), MAX(release_year) FROM film")
            result = cursor.fetchone()
            return result['MIN(release_year)'] or 1900, result['MAX(release_year)'] or 2025
    finally:
        conn.close()


def init_mongo():
    global mongo_collection
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        mongo_collection = db[MONGO_COLLECTION]
        mongo_collection.create_index([("timestamp", DESCENDING)])
        return True
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return False


def log_search(search_type, params, results_count):
    if mongo_collection is None:
        return
    try:
        mongo_collection.insert_one({
            "timestamp": datetime.now(),
            "search_type": search_type,
            "params": params,
            "results_count": results_count
        })
    except:
        pass


def get_statistics():
    if mongo_collection is None:
        return {"popular": [], "recent": []}
    try:
        popular = list(mongo_collection.aggregate([
            {"$group": {"_id": {"search_type": "$search_type", "params": "$params"},
                        "count": {"$sum": 1}, "last_searched": {"$max": "$timestamp"}}},
            {"$sort": {"count": DESCENDING}}, {"$limit": 10}
        ]))
        recent = list(mongo_collection.aggregate([
            {"$sort": {"timestamp": DESCENDING}},
            {"$group": {"_id": {"search_type": "$search_type", "params": "$params"},
                        "last_searched": {"$max": "$timestamp"}}},
            {"$sort": {"last_searched": DESCENDING}}, {"$limit": 5}
        ]))
        return {"popular": popular, "recent": recent}
    except:
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
            flash('Enter keyword', 'warning')
            return redirect(url_for('index'))

        movies, total, keyword_clean = search_by_keyword(keyword, page)


        if page == 1:
            log_search("keyword", {"keyword": keyword_clean}, len(movies))

        summary = {"type": "keyword", "description": f"Search: '{keyword_clean}'",
                   "page": page, "total": total,
                   "has_prev": page > 1, "has_next": (page * 10) < total}
        return render_template('search_results.html', movies=movies, summary=summary, keyword=keyword_clean)

    else:
        genre_id = int(request.args.get('genre_id'))
        start_year = int(request.args.get('start_year'))
        end_year = int(request.args.get('end_year'))

        movies, total, genre_name = search_by_genre_year(genre_id, start_year, end_year, page)


        if page == 1:
            log_search("genre_year", {"genre": genre_name, "years": f"{start_year}-{end_year}"}, len(movies))

        summary = {
            "type": "genre_year",
            "description": f"Genre: {genre_name}, Years: {start_year}-{end_year}",
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
    conn = get_mysql_connection()
    if not conn:
        return "DB error", 500
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM film WHERE film_id = %s", (film_id,))
            movie = cursor.fetchone()
            if not movie:
                flash('Film not found', 'error')
                return redirect(url_for('index'))
            return render_template('movie.html', movie=movie)
    finally:
        conn.close()


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
    try:
        if not init_mongo():
            print("⚠️  MongoDB failed, app runs without statistics")
        print("✅ App started! Open http://localhost:5000")
        app.run(debug=False, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped by user (Ctrl+C)")
        exit(0)