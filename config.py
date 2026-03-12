import os
from dotenv import load_dotenv


load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "password")
MYSQL_DB = os.getenv("MYSQL_DB", "sakila")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "final_project")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "movies")

SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
RESULTS_PER_PAGE = int(os.getenv("RESULTS_PER_PAGE", 10))
STATS_TOP_QUERIES = int(os.getenv("STATS_TOP_QUERIES", 5))