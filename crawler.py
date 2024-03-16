import os
import psycopg2
import psycopg2.extras
import feedparser
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv(override=True)


def fetch_rss_urls():
    # Connection details
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT', '25059')  # Default PostgreSQL port is 5432
    connect_timeout = 10  # Timeout in seconds

    # Print environment variables
    print(f"DB_HOST: {db_host}")
    print(f"DB_USER: {db_user}")
    # Do not print the password for security reasons
    # print(f"DB_PASSWORD: {db_password}")
    print(f"DB_NAME: {db_name}")
    print(f"DB_PORT: {db_port}")
    print(f"Connect Timeout: {connect_timeout}")
    # Connect to the PostgreSQL database with a timeout
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            connect_timeout=connect_timeout
        )
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return []

    # Create a cursor object
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Execute the query to fetch feed URLs and news_id
    cursor.execute("SELECT news_id, feed_url , category_id FROM rss")  # Replace 'your_table_name' with the actual table name
    
    # Fetch all URLs and news_ids
    feeds = cursor.fetchall()
    
    # Close the connection
    cursor.close()
    conn.close()
    
    # Return feeds
    return feeds

def classify_news(description):
    client = OpenAI(
        api_key= os.getenv('OPENAI_API_KEY')
    )
    chat_completion = client.chat.completions.create(
    messages=[
        {
            "role": "user",
            "content": f"Determine if the following news is good,bad or neutral: \"{description}\"",
        }
    ],
    model="gpt-3.5-turbo",
    max_tokens=100,
    temperature=0
)
    result = chat_completion.choices[0].message.content.lower()
    if "good" in result:
        return "good"
    elif "bad" in result:
        return "bad"
    elif "neutral" in result:
        return "neutral"
    else:
        return "neutral" 
    
    #for chunk in chat_completion:
    #    print(chunk.choices[0].delta.content or "", end="")
   

def parse_rss_type_1(feed_url):
    # Logic for parsing RSS feed with news_id 1
    feed = feedparser.parse(feed_url)
    articles = []
    for entry in feed.entries:
        article = {
            "title": entry.title,
            "description": entry.summary,
            "image": entry.media_content[0]['url'] if 'media_content' in entry else 'No image available',
            "url": entry.link
        }
        articles.append(article)
    return articles

def parse_rss_type_2(feed_url):
    # Logic for parsing RSS feed with news_id 2
    # This will be different based on the structure of the RSS feed for news_id 2
    feed = feedparser.parse(feed_url)
    articles = []
    # You will need to adjust the parsing logic to fit the structure of the RSS feed for news_id 2
    for entry in feed.entries:
        # Example of a different structure
        article = {
            "title": entry.title,
            "description": entry.summary,
            "url": entry.link
            # Assume there's no 'media_content' for news_id 2
        }
        articles.append(article)
    return articles

def insert_news(title, description, image_url, url, category_id):
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
    cursor = conn.cursor()

    # SQL query to insert data
    insert_query = """
    INSERT INTO news (title, news, image_url, url, category_id)
    VALUES (%s, %s, %s, %s, %s)
    """

    # Execute the query
    cursor.execute(insert_query, (title, description, image_url, url, category_id))

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

def fetch_and_parse_rss():
    feeds = fetch_rss_urls()
    
    for news_id, rss_url, category_id in feeds:
        if news_id == 1:
            articles = parse_rss_type_1(rss_url)
        elif news_id == 2:
            articles = parse_rss_type_2(rss_url)
        else:
            print(f"No parser for news_id {news_id}")
            continue
        
        # For demonstration, print the articles
        for article in articles:
            classification = classify_news(article['description'])
            print(f"Article: {article['title']}, Classification: {classification}")
            if classification in ["good", "neutral"]:
                insert_news(article['title'], article['description'], article.get('image', 'No image available'), article['url'], category_id)
            break

if __name__ == "__main__":
    fetch_and_parse_rss()
