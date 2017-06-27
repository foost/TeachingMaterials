#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Modified on Mon Jun 26 2017

@author: OstermannFO

flickrdatacollection_searchAPI.py: 
    access FlickrAPI
    write metadata as JSON objects in text file
    
currently using Python2; 
"""

import flickrapi
import datetime
import time
import sys
import os

#
# declare variables
#

# output path and file name trunk
PATH = ''
FILE_OUT_TRUNK = ''

# api_key
API_KEY = ''
API_SECRET = ''

# define search query keywords, dates (yyyy-mm-dd) and location;
# dates should be as 'YYYY-MM-DD'
# radius unit is km, default is 5
# bounding box min_lon, min_lat, max_lon, max_lat
# cannot use lat/lon/radius with empty bbox
SEARCH_QUERY = ''
START_DATE = ''
END_DATE = ''
LAT = ''
LON = ''
RADIUS = '' 
BBOX = '' 

# define search extras to be retrieved
SEARCH_EXTRAS = 'date_taken, date_upload, description, owner_name, geo, tags'

# flow control for this script
# tags_raw = 'yes' if raw tags should be retrieved; however, this makes 
# execution much slower; since queries do not return all photos all the time 
# (Flickr bug), queries with large results sets should not use this, but 
# instead use the same switch in the preprocessing script
# count_only = 'yes' to execute only initial query returning number of photos
TAGS_RAW = 'yes' 
COUNT_ONLY = 'no'

# list of values to be written to file
# owner_subelements_list currently not used
PHOTO_ATTR_LIST = ['id', 'title', 'owner', 'ownername', 'datetaken', 
                   'dateupload', 'latitude', 'longitude', 'accuracy', 
                   'granularity', 'tags']
PHOTO_SUBELEM_LIST = ['description']
#owner_subelements_list = ['realname', 'location'] 

# function to replace all problematic characters in the retrieved text
def replace_chars(text):
        text = text.replace('\r',' ')\
                    .replace('\n', ' ')\
                    .replace('\t', ' ')\
                    .replace(',', ' ')\
                    .replace("'", " ")\
                    .replace('"', ' ')\
                    #.replace('\\','/') #this one might clash with unicode
        return text
        
#
# start actual work
#
def main():    
    # create flickr instance
    flickr = flickrapi.FlickrAPI(API_KEY, API_SECRET)
    flickr.authenticate_via_browser(perms='read')
    
    # get total number of search results for info file
    if BBOX == '':
        search_results = flickr.photos_search(text = SEARCH_QUERY,
                        min_taken_date = START_DATE,
                        max_taken_date = END_DATE,
                        lat = LAT,
                        lon = LON,
                        radius = RADIUS)
    else:
        search_results = flickr.photos_search(text = SEARCH_QUERY,
                            min_taken_date = START_DATE,
                            max_taken_date = END_DATE,
                            bbox = BBOX)
    
    photos_element = search_results.find('photos')
    photos_query_total = photos_element.get('total')
    
    if COUNT_ONLY == 'yes':
        print photos_query_total
        sys.exit()
    
    # create new directory if necessary and write meta data to info file
    new_dir = "%s%s\\" %(PATH, FILE_OUT_TRUNK)
    if not os.path.exists(new_dir):
        try: 
            os.makedirs(new_dir)
        except: 
            print "Could not create new directory!"
            sys.exit()
    # info on search query
    f_info=open(new_dir + FILE_OUT_TRUNK + "_info.txt", 'w')
    f_info.write('Search started: ' + str(datetime.date.today()) + '\n')
    f_info.write('Search Query: ' + SEARCH_QUERY + '\n')
    f_info.write('Start Date: ' + START_DATE + '\n')
    f_info.write('End Date: ' + END_DATE + '\n')
    f_info.write('Lat , lon , bbox: ' + LAT + LON + BBOX + '\n')
    f_info.write('Extras retrieved: ' + SEARCH_EXTRAS + '\n')
    f_info.write('Raw tags retrieved: ' + TAGS_RAW + '\n')
    
    header = ""
    for column in PHOTO_ATTR_LIST:
        header = header + column + '\t'
    header = header + 'tags_raw\t'
    for column in PHOTO_SUBELEM_LIST:
        header = header + column + '\t'
    #for column in owner_subelements_list:
    #    header = header + column + '\t'
    header = header.rstrip('\t')
    
    f_info.write('Columns: ' + header + '\n')
    f_info.write('Number of photos in query: ' + photos_query_total + '\n')
    
    # start actual retrieval of data
    # first convert query dates to integer
    start_iter = datetime.datetime.strptime(START_DATE,"%Y-%m-%d").toordinal()
    end_iter = datetime.datetime.strptime(END_DATE,"%Y-%m-%d").toordinal()
    
    # initiate counters and list to filter out duplicates
    counter = 0
    ignored = 0
    committed = 0
    fid_list = []
    
    # end_iter +1 needed to get last day
    for i in range (start_iter, end_iter+1):
        
        # open daily output file
        query_date = datetime.date.fromordinal(i)
        f_results = open(new_dir + FILE_OUT_TRUNK + "_" + 
                         str(query_date) + ".tsv", 'w')
        f_results.write(header + "\n")
    
        # using single days retrieves more reliable results
        min_query_date = datetime.date.fromordinal(i-1)
        max_query_date = datetime.date.fromordinal(i+1)        
        if BBOX == '':
            search_results_daily = flickr.photos_search(text = SEARCH_QUERY,
                                    min_taken_date = min_query_date,
                                    max_taken_date = max_query_date,
                                    extras = SEARCH_EXTRAS,
                                    lat = LAT,
                                    lon = LON,
                                    radius = RADIUS)
        else:
            search_results_daily = flickr.photos_search(text = SEARCH_QUERY,
                                    min_taken_date = min_query_date,
                                    max_taken_date = max_query_date,
                                    extras = SEARCH_EXTRAS,
                                    bbox = BBOX)
        
        # to avoid rate limits, wait one second after api call
        time.sleep(1)
        photos_element_daily = search_results_daily.find('photos')
        pages_daily = photos_element_daily.get('pages')
        #photos_daily = photos_element_daily.get('total')
        
        # iterate over pages; it is possible to specify number of photos per 
        # page, but this is unreliable and does change maximum number of photos
        # per query (always 4000); therefore best to leave it at default 
        # (100 photos per page)
        page = 0
        while page <= int(pages_daily):    
            page = page + 1
            try:
                if BBOX == '':
                    search_results_daily_paginated = \
                        flickr.photos_search(text = SEARCH_QUERY,
                                             min_taken_date = min_query_date,
                                             max_taken_date = max_query_date,
                                             extras = SEARCH_EXTRAS,
                                             lat = LAT,
                                             lon = LON,
                                             radius = RADIUS,
                                             page = page)
                else:
                    search_results_daily_paginated = \
                        flickr.photos_search(text = SEARCH_QUERY,
                                             min_taken_date = min_query_date,
                                             max_taken_date = max_query_date,
                                             extras = SEARCH_EXTRAS,
                                             bbox = BBOX,
                                             page = page)
                                
                # to avoid rate limits, wait one second after api call
                time.sleep(1)
        
                # Iterate over photos in page
                photo_iter = \
                    search_results_daily_paginated.getiterator('photo')   
                for photo in photo_iter:
                    counter = counter + 1
                    try: 
                        fid = photo.get('id')
                        # check whether photo has already been processed
                        if fid in fid_list: 
                            ignored = ignored + 1
                            break
                        fid_list.append(fid)
                        out_row = fid.encode('utf-8') + '\t'             
                        
                        for attribute in PHOTO_ATTR_LIST[1:]:                
                            value = photo.get(attribute)             
                            # convert datetaken into posix timestamp
                            if attribute == 'datetaken':
                                value = \
                                    time.mktime(datetime.datetime.strptime( \
                                    value, "%Y-%m-%d %H:%M:%S").timetuple())
                                out_row = out_row + \
                                    str(int(value)).encode('utf-8') + '\t'
                            else:
                                if value is None:
                                    value = 'NODATA'
                                value = replace_chars(value)
                                out_row = out_row + value.encode('utf-8') + '\t'
                        
                        if TAGS_RAW == 'yes':
                            raw_tags = ''
                            tags = flickr.tags_getlistphoto(photo_id = fid)
                            # no wait possible here, otherwise takes too long
                            tag_iter = tags.getiterator('tag')
                            if tag_iter is None:
                                raw_tags = 'NODATA'
                            for tag in tag_iter:
                                raw_tag = tag.get('raw')
                                raw_tag = replace_chars(raw_tag)
                                raw_tags = raw_tags + raw_tag + "~"
                            raw_tags = raw_tags.rstrip('~')
                        else: 
                            raw_tags = "NOTQUERIED"
                        out_row = out_row + raw_tags.encode('utf-8') + '\t'
                            
                        for photo_subelement in PHOTO_SUBELEM_LIST:
                            value = photo.find(photo_subelement).text
                            if value is None:
                                value = 'NODATA'
                            else:
                                value = replace_chars(value)
                            out_row = out_row + value.encode('utf-8')   
                        
                        f_results.write(out_row + '\n')
                        committed = committed + 1
                    
                    except Exception as e: 
                        print "Problem with photo!", sys.exc_info()[0], str(e)
            
            except Exception as e:
                 print "Problem with search page!", sys.exc_info()[0], str(e)
                                          
        print query_date, photos_query_total, counter, ignored, committed, \
            len(fid_list)
        f_results.close()
    
    results_summary = 'Total retrieved/ignored/stored: %i/%i/%i' \
        %(counter, ignored, committed)
    print results_summary
    
    f_info.write(results_summary)    
    f_info.close()

if __name__=="__main__":
    main() 


