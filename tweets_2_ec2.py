# Stream Tweets from Twitter via EC2 and store in pymongo
import yaml
import os
import pymongo
from twitter import *

credentials = yaml.load(open(os.path.expanduser('api_cred.yml')))

consumer_secret = credentials['twitter'].get('consumer_secret')
consumer_key = credentials['twitter'].get('consumer_key')
access_token = credentials['twitter'].get('token')
access_token_secret = credentials['twitter'].get('token_secret')
client = pymongo.MongoClient('localhost',27017)
db = client.twitter_db
auth = OAuth(
    consumer_key= consumer_key,
    consumer_secret= consumer_secret,
    token= access_token,
    token_secret= access_token_secret)

twitter_userstream = TwitterStream(auth=auth, domain='userstream.twitter.com')
iterator = twitter_userstream.statuses.sample()
for tweet in iterator:
    db.tweets.insert_one(tweet)
