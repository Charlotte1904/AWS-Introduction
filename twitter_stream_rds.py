import os
import yaml
import psycopg2 # make sure it is in 2.6.2 version 
from psycopg2.extras import Json
from twitter import *

# CONNECT with Twitter 
credentials = yaml.load(open(os.path.expanduser('twitter_cred.yml')))

CONSUMER_SECRET = credentials['twitter']['consumer_secret']
CONSUMER_KEY = credentials['twitter']['consumer_key']
TOKEN = credentials['twitter']['token']
TOKEN_SECRET = credentials['twitter']['token_secret']
twitter_stream = TwitterStream(auth = OAuth(TOKEN, TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET))
iterator = twitter_stream.statuses.sample()


# CONNECT with psycopg -- PostgreSQL adapter
conn = psycopg2.connect(database="postgres", user="chaudao", password="chau1904", host="chautwitterpostgres.cy7p020zmkgu.us-west-2.rds.amazonaws.com", port="5432")
print "Opened database successfully"
cur = conn.cursor()

# load tweets to rds database 
counter = 0
for tweet in iterator:
    if "delete" not in tweet:
        cur.execute("""INSERT INTO raw_tweets (status) VALUES (%s)""", [Json(tweet)])
    counter += 1
    if counter % 50 == 0:
        conn.commit()
        print('Inserted {} tweets'.format(counter))
print "Records created successfully"
conn.close()