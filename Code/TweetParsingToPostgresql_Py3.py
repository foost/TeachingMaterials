#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Modified on 2019-04-04

@author: OstermannFO

tweetparsingtopostgresql.py: 
    reads JSON objects line by line
    parses them and puts desired values in database
    
necessary to create INSERT INTO statement manually
"""

import json
import psycopg2
import os
import sys
import codecs
import datetime

#
#declare variables
# 

CONN_DB = '' 
PATH = "path/to/files/"
IN_FILE_EXT = ".extensionToBeSearchedForInputFiles" 
LOG_FILE_NAME = "nameOfLogfileWithExtension"

def main():
    conn=psycopg2.connect(CONN_DB)
    cur=conn.cursor()
    
    log_file = codecs.open(LOG_FILE_NAME,'a','utf-8')
    
    for f in os.listdir(PATH):
        if f.endswith(IN_FILE_EXT):
            in_file_name = PATH + f
            in_file = codecs.open(in_file_name,'r','utf-8')  
            print("Now reading ... " + in_file_name)
            
            for line in in_file:                
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
                    data = (tweet_id,tweet_text,tweet_created,tweet_lon,
                            tweet_lat, user_id,user_name,user_folcount,
                            user_statcount,user_descr,user_loc,place_name,
                            place_id,)
                    try:
                        cur.execute(
                            """INSERT INTO tweet_db(tweet_id,tweet_text,
                            tweet_created,tweet_lon,tweet_lat,
                            user_id,user_name,user_folcount,user_statcount,
                            user_descr,user_loc,place_name,place_id) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                            """, data)
                        conn.commit()
						
                    except Exception as e:
                        conn.rollback()
                        print("DB error: ", sys.exc_info()[0], e)
                        log_file.write(str(datetime.datetime.today()) 
                                       + " DB Error in " + in_file_name 
                                       + " with tweet_id " + tweet_id + " : " 
                                       + str(e).replace("\n"," ") + "\n")
                except Exception as e:
                    print("Other Error: ", sys.exc_info()[0], e)
                    log_file.write(str(datetime.datetime.today()) 
                                    + " Other Error in " + in_file_name 
                                    + " with tweet_id " + tweet_id + " : " 
                                    + str(e).replace("\n"," ") + "\n")        
            
            in_file.close()
    
    conn.close()
    log_file.close()

if __name__=="__main__":
    main() 