import os
import yaml
import json
import pandas as pd

credentials = yaml.load(open(os.path.expanduser('api_cred.yml')))

# To get consumer key
# credentials['twitter'].get('consumer_key')
from twitter import *

t = Twitter(auth=OAuth(credentials['twitter'].get('token'), credentials['twitter'].get('token_secret'), 
    	credentials['twitter'].get('consumer_key'), credentials['twitter'].get('consumer_secret')))

twitter_stream = TwitterStream(auth=t)

#global trend - http://socialmedia-class.org/twittertutorial.html
world_trends = t.trends.available(_woeid=1)
sfo_trends = t.trends.place(_id = 2487956)


print (json.dumps(sfo_trends, indent=4))

# Convert first 10 trends to dataframe
pd_sf_report = pd.DataFrame(sfo_trends[0]['trends'][:10])

#export to html
pd_sf_report.to_html('top10.html')

