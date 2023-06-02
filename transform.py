import praw
import pandas as pd
import time
import psycopg2
import config  
import requests

user_agent = config.reddit_username
reddit = praw.Reddit(
    username=config.reddit_username,
    password=config.reddit_password,
    client_id=config.reddit_client_id,
    client_secret=config.reddit_client_secret,
    user_agent=user_agent,
    check_for_async=False
)

conn = psycopg2.connect(
    host=config.pg_host,
    database=config.pg_database,
    user=config.pg_user,
    password=config.pg_password
)
cur = conn.cursor()
# After establishing the connection and getting the cursor
# cur.execute("""
#     DROP TABLE IF EXISTS reddit_posts;
# """)
cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_posts_hot_summary (     
        subreddit TEXT,
        post_upvote_ratio REAL,
        post_comments INT,
        count int
    );
""")
conn.commit()