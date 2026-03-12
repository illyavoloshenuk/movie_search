# Movie Search Project

A web application for searching movies and tracking search statistics.  
Built as a final project for the ITCareerHub.de Python Developer program.

## Features

- Search movies using the MySQL *Sakila* sample database.
- Display paginated search results with a configurable number of results per page.
- Store user search queries in MongoDB and show basic statistics (top search queries).
- Simple and clean web interface built with Flask and Jinja2 templates.

## Tech Stack

- Python
- Flask
- MySQL (Sakila demo database)
- MongoDB
- HTML / CSS (templates & static files)
- python-dotenv for configuration via environment variables

## Project Structure

- app.py – main Flask application (routes, views, logic).
- config.py – application configuration (reads settings from environment variables).
- templates/ – HTML templates for pages.
- static/ – static assets (CSS, images, JS).
- requirements.txt – list of Python dependencies.

## Configuration

Sensitive settings (database credentials, secret key, etc.) are stored in environment variables.  
Create a .env file in the project root:

```env
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_USER=your-mysql-user
MYSQL_PASSWORD=your-mysql-password
MYSQL_DB=sakila

MONGO_URI=mongodb://localhost:27017/
MONGO_DB=final_project
MONGO_COLLECTION=movies

SECRET_KEY=your-secret-key
RESULTS_PER_PAGE=10
STATS_TOP_QUERIES=5


Installation:

# Clone the repository
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>

# Create virtual environment (optional)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

Runing the App:

# Make sure .env is created and databases are available

export FLASK_APP=app.py        # On Windows: set FLASK_APP=app.py
flask run