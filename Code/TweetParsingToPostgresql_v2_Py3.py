#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Modified on 2020-05-014

@author: OstermannFO

LondonTweetsProcessing
	reads JSON objects line by line
	parses them and puts desired values in database
	looks for extended Tweets
	ignores errors on single Tweets (e.g. duplicates)
	updates timestamp column

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

CONN_DB = ""
IN_PATH = ""
IN_FILE_EXT = ".txt" 
LOG_FILE_NAME = ""

def main():

	conn=psycopg2.connect(CONN_DB)
	cur=conn.cursor()

	log_file = codecs.open(LOG_FILE_NAME,'a','utf-8')

	for f in os.listdir(IN_PATH):
	
		if f.endswith(IN_FILE_EXT):
		
			in_file_name = IN_PATH + f
			in_file = codecs.open(in_file_name,'r','utf-8')
			print("Now reading ... " + in_file_name)

			for line in in_file:

				try:
				
					tweet = json.loads(line)
					tw_id = tweet['id_str']
					if 'extended_tweet' in tweet:
						tw_text = tweet['extended_tweet']['full_text']
					else:
						tw_text = tweet['text']
					tw_cr = tweet['created_at']
					if 'retweeted_status' in tweet:
						tw_rt = True
					else:
						tw_rt = False
					tw_lang = tweet['lang']
					if tweet['coordinates']:
						tw_lon = str(tweet['coordinates']['coordinates'][0])
						tw_lat = str(tweet['coordinates']['coordinates'][1])
					else:
						tw_lon = None
						tw_lat = None
					if tweet['place']:
						place_id = tweet['place']['id']
						place_name = tweet['place']['full_name']
					else:
						place_name = None
						place_id = None
					user_id = tweet['user']['id_str']
					user_loc = tweet['user']['location']

					data = (tw_id,tw_text,tw_rt,tw_cr,tw_lang,tw_lon,tw_lat,
							place_id,place_name,user_id,user_loc,json.dumps(tweet),)

					try:

						cur.execute(
							"""INSERT INTO tweet_table(tweet_id,tweet_text,
							tweet_retweet,tweet_created,tweet_lang,tweet_lon,tweet_lat,
							place_id,place_name,user_id,user_loc,tweet_json) 
							VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
							""", data)
						conn.commit()

					except Exception as e:

						conn.rollback()
						print("DB Error: ", sys.exc_info()[0], e)
						log_file.write(str(datetime.datetime.today())
									   + " Error in " + in_file_name
									   + " with tweet_id " + tw_id + " : "
									   + str(e).replace("\n", " ") + "\n")

				except Exception as e:

					print("Other Error: ", sys.exc_info()[0], e)
					log_file.write(str(datetime.datetime.today())
									+ " Error in " + in_file_name
									+ " with tweet_id " + tw_id + " : "
									+ str(e).replace("\n"," ") + "\n")

			in_file.close()

	try:

		cur.execute(
			"""SET TIME ZONE '+0';
			UPDATE tweet_table SET tw_cr_tstz = 
			to_timestamp(tweet_created, 'Dy Mon DD HH24:MI:SS +0000 YYYY') 
			WHERE tw_cr_tstz IS NULL;""")
		conn.commit()

	except Exception as e:

		conn.rollback()
		print("Update timestamp Error: ", sys.exc_info()[0], e)
		log_file.write(str(datetime.datetime.today())
					   + " Update timestamp Error: " +
					   str(e).replace("\n", " ") + "\n")

	conn.close()
	log_file.close()

if __name__=="__main__":
	main()