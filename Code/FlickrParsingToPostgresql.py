#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
@author: OstermannFO

FlickrParsingToPostgresql_London.py: 
    reads Flickr photo metadata line by line
    parses them and puts desired values in database
    
currently still using Python2;
necessary to create INSERT INTO statement manually
"""

import psycopg2
import os
import sys
import datetime

#
#declare variables
# 

CONN_DB = ""
PATH = "path/to/files"
IN_FILE_EXT = ".extensionToBeSearchedForInputFiles" 
LOG_FILE_NAME = "nameOfLogFile"

def main():
    conn=psycopg2.connect(CONN_DB)
    cur=conn.cursor()
    
    log_file = open(LOG_FILE_NAME,'a')
    
    for f in os.listdir(PATH):
        if f.endswith(IN_FILE_EXT):
            in_file_name = PATH + f
            in_file = open(in_file_name,'r')
            print "Now reading ... " + in_file_name
            next(in_file)
            
            for line in in_file:
                
                try:
                    fields = line.split('\t')                   
                    fid = fields[0]
                    title = fields[1]
                    uid = fields[2]
                    uname = fields[3]
                    date_up = fields[4]
                    date_taken = fields[5]
                    lat = fields[6]
                    lon = fields[7]
                    acc = fields[8]
                    tags_flickr = fields[9]
                    tags_user = fields[10]
                    descr = fields[11]
    
                    data = (fid,title,uid,uname,date_up,date_taken,lat,lon,
                            acc,tags_flickr,tags_user,descr,)
                    
                    try: 
                        
                        cur.execute(
                            """INSERT INTO flickr_table (FID,TITLE,UID,UNAME,
                            DATE_UP,DATE_TAKEN,LAT,LON,ACC,
                            TAGS_FLICKR,TAGS_USER,DESCR) VALUES 
                            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                            """, data)
                        conn.commit()
                        
                    except Exception,e:
                        conn.rollback()
                        print "DB Error: ", sys.exc_info()[0], e
                        log_file.write(str(datetime.datetime.today()) 
                                       + " DB Error in " + in_file_name 
                                       + " with flickr_id " + fid + " : " 
                                       + str(e).replace("\n"," ") + "\n")
                    
                except Exception,e:
                    print "Other Error: ", sys.exc_info()[0], e
                    log_file.write(str(datetime.datetime.today()) 
                                    + " Other Error in " + in_file_name 
                                    + " with flickr_id " + fid + " : " 
                                    + str(e).replace("\n"," ") + "\n")
            in_file.close()
    
    cur.execute(
            """UPDATE flickr_table SET the_geom = ST_SetSRID(
            ST_Point(lon,lat),4326) WHERE the_geom IS NULL""")        
    conn.commit()
    conn.close()
    log_file.close()

if __name__=="__main__":
    main() 