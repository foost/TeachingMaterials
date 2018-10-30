#! /usr/bin/python
# coding: utf-8

"""
Modified on Mon Jun 26 2017

@author: OstermannFO

foursquaredatacollection_searchAPI.py: 
    access FoursquareAPI
    write selected metadata in text file
    
currently using Python2; 
This script is based on the information contained in the Foursquare API and the
Python wrapper documentation. 
https://developer.foursquare.com/start/search
https://github.com/mLewisLogic/foursquare
It seems that the most suitable method is search, which does not require user 
authentification:
https://developer.foursquare.com/docs/venues/search

Querying the API using a point grid to work around the limitation of 50 
returned results per query. However, watch out for rate limits of 5000 
userless queries per hour.
"""

import foursquare
import csv

#
# declare variables
#

# API keys
CLIENT_ID = ''
CLIENT_SECRET = ''

# pointgrid file for input
POINTGRID = ''
RADIUS = 500
OUTPUTCSV = ''
PATH = ''

def main():
    client = foursquare.Foursquare(client_id = CLIENT_ID,
                                   client_secret = CLIENT_SECRET)    
    with open('{}{}'.format(PATH, POINTGRID), 'rb') as csvin:
        filereader = csv.reader(csvin)
        next(filereader)
        with open('{}{}'.format(PATH, OUTPUTCSV), 'wb') as csvout:
            filewriter = csv.writer(csvout)
            fields = ['vid', 'name', 'lat', 'lon', 'category', 'tipCount', 
                      'checkinsCount', 'usersCount']
            filewriter.writerow(fields)
            place_ids = []
            for rowin in filereader:
                lon = rowin[0]
                lat = rowin[1]
                pid = rowin[2]
                print pid,
                venues = client.venues.search(params={'ll': '%s, %s' %(lat,lon),
                                                      'intent': 'browse', 
                                                      'radius': '%s' %(RADIUS)})
                for venue in venues['venues']:
                    if venue['id'] not in place_ids:
                        name = venue['name'].encode('utf-8')
                        vid = venue['id']
                        lat = venue['location']['lat']
                        lon = venue['location']['lng']
                        tips = venue['stats']['tipCount']
                        checkins = venue['stats']['checkinsCount']
                        users = venue['stats']['usersCount']
                        place_ids.append(vid)
                        try:
                            category = \
                                venue['categories'][0]['shortName'].encode('utf-8')
                        except IndexError: 
                            category =''
                        rowout = [vid, name, lat, lon, category, tips, 
                                  checkins, users]
                        filewriter.writerow(rowout)
            
if __name__=="__main__":
    main() 


