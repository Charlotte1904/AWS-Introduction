import boto3
import os
import yaml
import json
from twitter import TwitterStream, OAuth


# Connect to firehose
client = boto3.client('firehose',region_name = 'us-west-2')

# Connect to Twitter
credentials = yaml.load(open(os.path.expanduser( 'twitter_cred.yml')))
twitter_stream = TwitterStream(auth = OAuth(**credentials['twitter']))

#Streamming Tweets
iterator = twitter_stream.statuses.sample()

#Loading tweets to the firehose
counter = 0
for tweet in iterator:
	if "delete" not in tweet:
		response = client.put_record(DeliveryStreamName='chau_firehose',
			Record={'Data': json.dumps((tweet)+'\n')})
		counter += 1
	if counter % 50 == 0:
		print('Inserted {} tweets'.format(counter))


