import json
import pandas as pd
from pyspark import SparkContext

sc = SparkContext()

# import files from S3 bucket 
twitterRDD = sc.textFile("s3a://chaufirehosebucket/2017/02/10/23")

def check_if_valid(tweet):
    try:
        tweet_json = json.loads(tweet)
        
        if 'delete' in tweet or 'entities' not in tweet:
            return False
            
        if len(tweet_json['entities']['hashtags']) == 0:
            return False
        
        return True
        
    except ValueError:
        return False
        
def convert_to_json(tweet):
    return json.loads(tweet)
    
def get_hashtags(tweet_json):
    hashtag = tweet_json['entities']['hashtags'][0]['text']
    
    return hashtag, 1


twitterRDDsplit = twitterRDD.flatMap(lambda x: x.split("\n")).filter(check_if_valid).map(convert_to_json).map(get_hashtags).reduceByKey(lambda x, y: x+y)
results = twitterRDDsplit.takeOrdered(20, lambda x: -x[1])

top_10_rdd = sc.parallelize(results)
top_hashtags = pd.DataFrame(results, columns=['hashtag_count', 'hashtag'])
html_format = top_hashtags.to_html()

with open("output_1.text", "w+") as f:
    for result in results:
        f.write(result[0].encode('UTF-8') + "\t" + str(result[1]) + "\n")
    
html = "<!DOCTYPE html><html><body>{}</body></html>".format(html_format.encode('utf-8'))
output_html = open("html_format.html", 'w')
output_html.write(html)
output_html.close()

top_10_rdd.saveAsTextFile('s3a://chausparkmr')




# ssh into EMR -- install pandas 
# scp thisfile.py 
# Run unset PYSPARK_DRIVER_PYTHON to avoid using jupyter command
# To run this we use following command "spark-submit tweets.py"
# this will generate output.text in the EMR node & html file 
# to download file.html to local machine 
# scp -i /Users/Charlotte/.ssh/twitter_ec2.pem hadoop@ec2-52-36-186-203.us-west-2.compute.amazonaws.com:~/html_format /Users/Charlotte/Desktop
# then on local machine run "python upload_file_s3.py.py html_format.html"

        
