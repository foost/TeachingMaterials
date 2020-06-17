#!/usr/bin/python

"""
Modified on 2019-01-08

@author: OstermannFO

TweetParsingToCSV.py: 
    reads JSON objects line by line
    parses them and puts desired values in CSV

Python 3 unicode character treatment not validated!;
only works for Tweets in compatibility mode (e.g. collected
through standard Streaming API), because it looks for a "text"
field and not a "full_text" field, therefore it does not parse
any extended Tweets
"""

import json
import os
import codecs
import sys
import csv

inpath = '\\path\\to\\working\\directory\\'
outfile = '\\path\\to\\working\\directory\\'

header = "tweet_id,tweet_text,tweet_created,tweet_lon,tweet_lat,user_id,user_name,user_folcount,user_statcount,user_descr,user_loc,place_name,place_id\n"

outfile_handle = open(outfile,'w')
outfile_handle.write(header)

csvout = csv.writer(outfile_handle)

for f in os.listdir(inpath):
    if f.endswith(".txt"):
        twitter_file = open(inpath+f,'r')
        print ("Now reading ... " + str(f))
        try:
            for line in twitter_file:
                try:
                    Tweet = json.loads(line)
                    tweet_id=str(Tweet['id'])
                    tweet_text=Tweet['text']
                    tweet_created=Tweet['created_at']
                    if Tweet['coordinates']:
                        tweet_lon=str(Tweet['coordinates']['coordinates'][0])
                        tweet_lat=str(Tweet['coordinates']['coordinates'][1])
                    else:
                        tweet_lon=None
                        tweet_lat=None
                    user_id=Tweet['user']['id_str']
                    user_name=Tweet['user']['screen_name']
                    user_folcount=str(Tweet['user']['followers_count'])
                    user_statcount=str(Tweet['user']['statuses_count'])
                    user_descr=Tweet['user']['description']
                    user_loc=Tweet['user']['location']
                    if Tweet['place']:
                        place_name=Tweet['place']['full_name']
                        place_id=Tweet['place']['id']
                    else:
                        place_name=None
                        place_id=None
                    rowout = [tweet_id,tweet_text,tweet_created,
                              tweet_lon,tweet_lat,user_id,user_name,
                              user_folcount,user_statcount,user_descr,user_loc,
                              place_name,place_id]
                    csvout.writerow(rowout)
                except Exception as e:
                    print ("Something wrong with Tweet!", sys.exc_info()[0], str(e))
        except Exception as e:
            print ("Unexpected error, skipped!", sys.exc_info()[0], str(e), line[:40])
        twitter_file.close()
    else:
        continue
        
outfile_handle.close()