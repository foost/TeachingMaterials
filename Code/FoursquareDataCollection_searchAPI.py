#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
@author: OstermannFO

FoursquareDataCollection_searchAPI.py: 
    access FoursquareAPI
	store results in CSV
    
"""

import foursquare
import csv

#
# declare variables
#

# API keys
CLIENT_ID = ''
CLIENT_SECRET = ''

# output file
OUTPUT_FILE = ''

def main():

	client = foursquare.Foursquare(client_id=CLIENT_ID,
								   client_secret=CLIENT_SECRET)

	venues = client.venues.search(params={'query': 'coffee', 'll': '-37.8142176,144.9631608'})
	print (venues)

	with open(OUPTUT_FILE, encoding = 'utf-8', mode = 'w', newline = '') as csvout:
			filewriter = csv.writer(csvout)
			fields = ['vid', 'name', 'lat', 'lon', 'category']
			filewriter.writerow(fields)
			place_ids = []
			for venue in venues['venues']:
				if venue['id'] not in place_ids:
					name = venue['name']
					vid = venue['id']
					lat = venue['location']['lat']
					lon = venue['location']['lng']
					place_ids.append(vid)
					try:
						category = venue['categories'][0]['shortName']
					except IndexError: 
						category =''
					rowout = [vid, name, lat, lon, category]
					filewriter.writerow(rowout)
					
if __name__=="__main__":
    main() 