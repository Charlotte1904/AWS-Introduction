import yaml
import os
import pymongo
import psycopg2
import pandas as pd

credentials = yaml.load(open(os.path.expanduser('rds_cred.yml')))
conn = psycopg2.connect(**credentials['rds'])
print "Opened database successfully"
cur = conn.cursor()

# Create a materialized view
# CREATE MATERIALIZED VIEW table as
# SELECT CAST((regexp_replace(CAST(status AS text), '\\u0000', '', 'g')) AS json) ->> 'id' AS tweeter_id,
# CAST(json_array_elements(CAST((regexp_replace(CAST(status AS text), '\\u0000', '', 'g')) as json) -> 'entities' -> 'hashtags') -> 'text' AS text) AS raw_hashtag,
# LOWER(CAST(json_array_elements(CAST((regexp_replace(CAST(status AS text), '\\u0000', '', 'g')) as json) -> 'entities' -> 'hashtags') -> 'text' AS text)) AS normalized_hashtag
# FROM raw_tweets;


cur.execute("""SELECT normalized_hashtag, count(normalized_hashtag)
    FROM hashtag_table
    GROUP BY normalized_hashtag
    ORDER BY count(normalized_hashtag)
    DESC
    LIMIT 10;""")

top10_hashtag = pd.DataFrame(cur.fetchall(), columns=['hashtags', 'count'])

html = "\
<html> \
 <head></head> \
 <body>\
   <p>Yo!<br>\
      This is the Top 10 trending hashtag. <br>{}\
   </p>\
 </body>\
</html>\
".format(top10_hashtag.to_html())
cur.close()

with open('top10_hashtag.html', 'w') as f:
    f.write(html)

print "Records created successfully"
conn.close()


#http://ec2-52-37-183-147.us-west-2.compute.amazonaws.com/top10_hashtag.html
