#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Modified on Fri Jul 14 2017

@author: OstermannFO

BuildTermVectors.py: 
    builds term vectors by lexical matching of stems
    
table in database needs to have vector fields ready and empty
INSERT statement and other database-related parameters need to be 
manually adjusted
terms to be loaded from one or more files (one keyword per line), which also 
need to have been stemmed
"""

#
# import statements
#
import re
import psycopg2
import sys
import datetime
import nltk

#
#declare user variables in CAPITALS
# 
CONN_DB = "" 
PATH = ""
TERM_FILES_NAMES = [] 
LOG_FILE = "BuildTermVector"


def main():         
    conn = psycopg2.connect(CONN_DB)
    cur = conn.cursor('server_side_cursor', withhold = True) #for big result sets
    cur2 = conn.cursor()
    stemmer = nltk.PorterStemmer()

    log_file_name = PATH + LOG_FILE + ".log"
    log_file = open(log_file_name,'a')
    
    term_dicts = []
    term_dicts_index = 0

    # read in the terms to look for
    for term_file in TERM_FILES_NAMES:
        term_dicts.append({})
        file_handle = open(PATH + term_file)
        dict_index = 0
        for line in file_handle:
            term_dicts[term_dicts_index][line.rstrip()] = dict_index
            dict_index += 1
        file_handle.close()
        term_dicts_index += 1
    try: 
        # get ids from items to use, needs manual configuration
        cur.execute("SELECT uid FROM table WHERE some = condition")
        uids=cur.fetchall()    
    except Exception,e:
        uids = []    
        print "DB error: ", sys.exc_info()[0], e
        log_file.write(str(datetime.datetime.today()) 
                        + " DB Error SELECT while fetching records: " 
                        + str(e).replace("\n"," ") + "\n")
   
    number_records = len(uids)
    counter=1
    pattern = re.compile('[\W_]+', re.UNICODE) #remove problematic characters
    # iterate over results
    for uid in uids:
        str_uid = str(uid[0])
        input_unigrams = []
        found_terms = []
        term_vectors = []
        # create empty term vectors
        for term_dict in term_dicts:
            term_vector = [0] * len(term_dict)
            term_vectors.append(term_vector)
             
        try:
            # retrieve input text to look for terms, needs manual configuration
            cur2.execute("SELECT text FROM table WHERE uid = '"
                        + str_uid + "'")
            input_text = cur2.fetchall()
        except Exception,e:
            conn.rollback()
            print "DB error: ", sys.exc_info()[0], e
            log_file.write(str(datetime.datetime.today()) 
                           + " DB Error SELECT Tweet id " 
                           + str_uid + " : " 
                           + str(e).replace("\n"," ") + "\n")
            print str(counter) + " of " + str(number_records) 
            counter += 1
            continue
        
        unigrams = unicode(input_text[0]).split(" ")
        # build list of input unigrams
        for unigram in unigrams:
            input_unigrams.append(pattern.sub('', unigram.lower()))
        # look for matching terms and update term vector(s)
        for input_unigram in input_unigrams:
            input_unigram_stemmed = stemmer.stem(input_unigram)
            for x in range(0,len(term_dicts)):  
                if input_unigram_stemmed in term_dicts[x]:
                    term_vectors[x][term_dicts[x][input_unigram_stemmed]] += 1
                    found_terms.append(input_unigram_stemmed)
        if not found_terms:
            found_terms = '0'
        else:
            found_terms = ','.join(str(s) for s in found_terms)
        # create input for SQL statement
        data = ()
        for x in range(0,len(term_dicts)):
            data = data + ((' '.join(str(i) for i in term_vectors[x])),)
        data = data + (found_terms, str_uid)
        try:  
            cur2.execute("""UPDATE table
                        SET (%s) = 
                        (%s) 
                        WHERE uid = (%s);""", data) # needs manual update
            conn.commit()
        except Exception,e:
            conn.rollback()
            print "DB error: ", sys.exc_info()[0], e
            log_file.write(str(datetime.datetime.today()) 
                           + " DB Error UPDATE Tweet id " 
                           + str_uid + " : " 
                           + str(e).replace("\n"," ") + "\n")    
        
        print str(counter) + " of " + str(number_records) 
        counter += 1
         
    conn.close()    
    log_file.close()

if __name__=="__main__":
    main() 