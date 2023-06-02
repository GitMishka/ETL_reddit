import time
import praw
import pandas as pd
import psycopg2
from psycopg2 import sql

# Create a connection to your PostgreSQL database
conn = psycopg2.connect(
    dbname="your_db",
    user="your_username",
    password="your_password",
    host="your_host",
    port="your_port"
)

# Create a new Reddit instance
reddit = praw.Reddit(
    client_id="my_client_id",
    client_secret="my_client_secret",
    user_agent="my_user_agent",
)

def fetch_data():
    # Fetch the top 100 posts from /r/all
    posts = reddit.subreddit('all').new(limit=100)

    # Create a DataFrame to hold all the posts
    posts_df = pd.DataFrame()

    for post in posts:
        content = {
            'post_id': post.id,
            'post_title': post.title,
            'subreddit': post.subreddit.display_name,
            'post_upvote_ratio': post.upvote_ratio,
            'post_comments': post.num_comments,
            'post_timeposted': post.created_utc,
        }
        posts_df = posts_df.append(content, ignore_index=True)
    
    return posts_df

while True:
    try:
        # Fetch the data
        df = fetch_data()

        # Upsert the data to your PostgreSQL database
        df.to_sql('reddit_posts', conn, if_exists='replace', index=False)

        # Make sure to commit the transaction
        conn.commit()

        # Wait for a minute
        time.sleep(60)

    except Exception as e:
        print(str(e))
        # If there is an error in the connection, we should roll it back
        conn.rollback()
        # Wait for a minute before the next fetch
        time.sleep(60)
