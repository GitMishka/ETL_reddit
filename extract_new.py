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

cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_posts_new (
        post_id TEXT PRIMARY KEY,
        post_title TEXT,
        subreddit TEXT,
        post_upvote_ratio REAL,
        post_comments INT,
        post_timeposted TIMESTAMP
    );
""")
conn.commit()  

import datetime

def fetch_data():
    posts = reddit.subreddit('all').new(limit=100)

    posts_list = []

    for post in posts:
        content = {
            'post_id': post.id,
            'post_title': post.title,
            'subreddit': post.subreddit.display_name,
            'post_upvote_ratio': post.upvote_ratio,
            'post_comments': post.num_comments,
            'post_timeposted': datetime.datetime.utcfromtimestamp(post.created_utc),
        }
        posts_list.append(content)
    
    posts_df = pd.DataFrame(posts_list)
    return posts_df


def upsert_post(post):
    insert = """
    INSERT INTO reddit_posts_new (post_id, post_title, subreddit, post_upvote_ratio, post_comments, post_timeposted) 
    VALUES(%s, %s, %s, %s, %s, %s) 
    ON CONFLICT (post_id) 
    DO UPDATE SET 
    post_title = excluded.post_title,
    subreddit = excluded.subreddit,
    post_upvote_ratio = excluded.post_upvote_ratio,
    post_comments = excluded.post_comments,
    post_timeposted = excluded.post_timeposted
    """
    cur.execute(insert, post)

while True:
    try:
        df = fetch_data()

        for index, row in df.iterrows():
            upsert_post(row.tolist())

        conn.commit()

        time.sleep(60)

    except Exception as e:
        print(str(e))
        conn.rollback()
        time.sleep(60)
