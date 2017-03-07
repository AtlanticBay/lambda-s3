from __future__ import print_function
from mongoengine import *
import json, urllib, boto3, datetime

print('Loading function')

s3 = boto3.client('s3')
as3 = boto3.resource('s3')

class logs(Document):
    fullPath = StringField(required=True, max_length=200)
    fileName = StringField(required=True, max_length=200)
    url = StringField()
    dateCreated = DateTimeField()
    errorCount = IntField()

class jobs(Document):
    taskName = StringField(required=True, max_length=200)

class error(Document):
    errorDate = DateTimeField()
    errorPath = StringField()
    errorText = StringField()

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    # Connect to mongo
    connect('hztest', host='ec2-52-91-110-103.compute-1.amazonaws.com', port=27017)
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        obj = as3.Object(bucket, key)
        fileName=key[24::]
        sitePrefix='https://s3.amazonaws.com/informatica-logs/'
        url=sitePrefix+key
        dataFile=obj.get()["Body"].read().decode('utf8')
        if dataFile.count('/n')>0:
            cnt=(dataFile.count('\n')-1)
        else: 
            cnt=0
        lastModified=obj.get()['ResponseMetadata']['HTTPHeaders']['x-amz-meta-lastwritetime']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    try:
         post=logs(fullPath = key,
                       fileName = fileName,
                       url = url,
                       dateCreated = lastModified,
                       errorCount = cnt)
         post.save()
         success= print(post)
         return success
    except Exception as z:
        raise z
