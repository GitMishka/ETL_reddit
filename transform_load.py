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
conn2 = psycopg2.connect(
    host=config.pg_host,
    database="db1",
    user=config.pg_user,
    password=config.pg_password
)
cur2 = conn2.cursor()

cur2.execute("""
    CREATE TABLE IF NOT EXISTS reddit_posts_hot_summary (     
        subreddit TEXT,
        post_upvote_ratio FLOAT,
        post_comments INT,
        count int
    );
""")
conn2.commit()
cur.execute("""
    SELECT subreddit, AVG(post_upvote_ratio) avg_upvote_ratio
    ,ROUND(AVG(reddit_posts_hot.post_comments)) avg_comment_count
    ,COUNT(subreddit) 
    FROM reddit_posts_hot GROUP BY 1 ORDER BY 2 DESC
""")
rows = cur.fetchall()

for row in rows:
    cur2.execute("""
        INSERT INTO reddit_posts_hot_summary (subreddit, post_upvote_ratio, post_comments, count)
        VALUES (%s, %s, %s, %s)
    """, (row[0], row[1], row[2], row[3]))
conn2.commit()

