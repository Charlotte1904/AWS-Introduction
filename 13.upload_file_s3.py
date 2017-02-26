import ssl
import boto
from boto.s3.connection import S3Connection

def boto_upload_s3(html_file):
    conn = S3Connection()
    if hasattr(ssl, '_create_unverified_context'):
        ssl._create_default_https_context = ssl._create_unverified_context
    website_bucket = conn.create_bucket('chausparksubmitresults')
    website_bucket.set_policy('''{
  "Version":"2012-10-17",
  "Statement": [{
    "Sid": "Allow Public Access to All Objects",
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::%s/*"
  }
 ]
}''' % website_bucket.name)

    # website_bucket = conn.get_bucket('sparksubmitresults')
    output_file = website_bucket.new_key('top20_spark.html')
    output_file.content_type = 'text/html'
    output_file.set_contents_from_string(html_file, policy='public-read')

if __name__ == '__main__':
	top20 = open("html_format.html", "r")
	html_file = top20.read()
	boto_upload_s3(html_file)
	top20.close()
