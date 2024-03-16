import os
import psycopg2
import psycopg2.extras
import feedparser
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def fetch_rss_urls():
    # Connection details
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT', '5432')  # Default PostgreSQL port is 5432

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    
    # Create a cursor object
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Execute the query
    cursor.execute("SELECT url FROM rss_feeds")
    
    # Fetch all URLs
    urls = cursor.fetchall()
    
    # Close the connection
    cursor.close()
    conn.close()
    
    # Return URLs
    return [url[0] for url in urls]

def fetch_and_parse_rss():
    urls = fetch_rss_urls()
    
    for rss_url in urls:
        feed = feedparser.parse(rss_url)
        articles = []
        
        for entry in feed.entries:
            article = {
                "title": entry.title,
                "description": entry.summary,
                "image": entry.media_content[0]['url'] if 'media_content' in entry else 'No image available',
                "url": entry.link
            }
            articles.append(article)
        
        # For demonstration, print the articles
        for article in articles:
            print(article)

if __name__ == "__main__":
    fetch_and_parse_rss()
