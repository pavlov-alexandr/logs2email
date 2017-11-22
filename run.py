import json
import boto3
import logging
import os
import pickle
import pandas as pd
from datetime import datetime
from dateutil import parser

logname = "logs2email"
logger = logging.getLogger(logname)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.INFO)

hdlr = logging.FileHandler('/var/tmp/' + logname + '.log')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 

stdout = logging.StreamHandler()
stdout.setFormatter(formatter)
logger.addHandler(stdout)

lr_filename = "last_run.txt"

def get_last_run():
    if os.path.exists(lr_filename):
        n = pickle.load(open(lr_filename, 'rb'))
    else:
        n = datetime(2017,1,1)
    return n

def last_run_to_log_format(lr):
    prefix = ""
    n = datetime.now()
    if lr.year == n.year:
        prefix += str(lr.year) + "-"
        if lr.month == n.month:
            prefix += str(lr.month).zfill(2) + "-"
            if lr.day == n.day:
                prefix += str(lr.day).zfill(2) + "-"
                if lr.hour == n.hour:
                    prefix += str(lr.hour).zfill(2) + "-"
                    if lr.minute == n.minute:
                        prefix += str(lr.minute).zfill(2) + "-"
                        if lr.second == n.second:
                            prefix += str(lr.second).zfill(2) + "-"
    else:
        # Fix this bug
        prefix = str(n.year) + "-"
    return prefix

def save_last_updated(n):
    f = open(lr_filename, 'wb')
    pickle.dump(n, f)
    f.close()


def is_new(obj, last_run):
    f = len(folder)
    x = obj.key[f:f+23]
    s = x[0:10] + ' ' + x[11:19].replace('-', ':') + '.' + x[20:]
    dt = parser.parse(s)
    return dt > last_run


def contents_to_string(obj):
    a = obj.get()['Body'].read().decode('utf-8')
    arr = a.split('\n')
    items = []
    for item in arr:
        if item != '':
            b = json.loads(item)
            items.append(b)
    return items


def content_to_html(s3key, items):
    df = pd.DataFrame(items)
    del df['level']
    df['message'] = df['message'].apply(lambda x: "{}" if x[0:1] != '{' else x)
    subrows = []
    for msg in df['message']:
        o = json.loads(msg)
        subrows.append(o)
    df2 = pd.DataFrame(subrows)
    del df['message']
    df3 = pd.concat([df, df2], axis=1)
    df3['timestamp'] = df3['timestamp'].apply(lambda x: str(x)[-13:])
    html = "<h1>" + s3key + "</h1>" + df3.to_html()
    return html


def send_email(ses, recipient, sender, subject, bdy):
    charset = "UTF-8"
    response = ses.send_email(
        Destination={
            'ToAddresses': [
                recipient,
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': charset,
                    'Data': bdy,
                },
                'Text': {
                    'Charset': charset,
                    'Data': "Remote requires HTML viewer",
                },
            },
            'Subject': {
                'Charset': charset,
                'Data': subject,
            },
        },
        Source=sender
    )


if __name__ == '__main__':
    config = json.load(open('config/config.json', 'r'))
    accessKey = config['accessKey']
    secretKey = config['secretKey']
    bucketname = config['bucket_name']
    folder = config['folder']
    s3 = boto3.resource('s3', aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
    ses = boto3.client('ses', aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
    mybucket = s3.Bucket(bucketname)
    #
    last_run = get_last_run()
    file_prefix = last_run_to_log_format(last_run)
    prefix = folder + file_prefix
    logger.debug("Getting with prefix: " + prefix)
    n = datetime.now()
    objs = mybucket.objects.filter(Prefix=prefix)
    bdy = ""
    for obj in objs:
        if is_new(obj, last_run):
            jrows = contents_to_string(obj)
            h = content_to_html(obj.key, jrows)
            bdy += h
    recipient = "kyle@dataskeptic.com"
    sender = "kyle@dataskeptic.com"
    subject = "Bot Logs at " + str(n)
    send_email(ses, recipient, sender, subject, bdy)
    save_last_updated(n)
