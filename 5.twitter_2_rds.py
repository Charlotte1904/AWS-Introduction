import os
import yaml
import psycopg2 # make sure it is in 2.6.2 version 
from psycopg2.extras import Json
from twitter import *

# CONNECT with Twitter 
credentials = yaml.load(open(os.path.expanduser( 'twitter_cred.yml')))
twitter_stream = TwitterStream(auth = OAuth(**credentials['twitter']))
iterator = twitter_stream.statuses.sample()


# CONNECT with psycopg -- PostgreSQL adapter
rds_credentials = yaml.load(open(os.path.expanduser('rds_cred.yml')))
conn = psycopg2.connect(**rds_credentials['rds'])
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
