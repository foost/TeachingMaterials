#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
@author: OstermannFO

builds term vectors by lexical matching, using stemmed terms
    
table in database needs to have vector fields ready and empty;
INSERT statement and other database-related parameters need to be 
manually adjusted
v2 simplified to use only one term list and create only one vector
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
TERM_FILE = ""
LOG_FILE = ""

def main():

    conn = psycopg2.connect(CONN_DB)
    cur = conn.cursor('server_side_cursor', withhold = True)
    cur2 = conn.cursor()
    stemmer = nltk.PorterStemmer()

    log_file_name = PATH + LOG_FILE
    log_file = open(log_file_name,'a')
    
    # create dictionary with term:index_number (needed for later adding up found terms
    term_dict = {}
    file_handle = open(PATH + TERM_FILE)
    dict_index = 0
    for line in file_handle:
        term_dict[line.rstrip()] = dict_index
        dict_index += 1
    file_handle.close()
    
    # collect ids of entries to process
    try:
        cur.execute("SELECT id FROM table WHERE terms_found IS NULL")
        gids=cur.fetchall()            
    except Exception as e:
        print ("DB error: ", sys.exc_info()[0], e)
        log_file.write(str(datetime.datetime.today()) 
                       + " DB Error SELECT while fetching records: " 
                       + str(e).replace("\n"," ") + "\n")
        gids = []
    
    number_records = len(gids)
    counter = 1
    
    # create pattern to remove unwanted characters from input string
    pattern = re.compile('[\W_]+')
    
    for gid in gids:

        str_gid = str(gid[0])
        input_unigrams = []
        found_terms = []
        term_vector = [0] * len(term_dict)
        
        # retrieve data for finding terms
        try:
            cur2.execute('SELECT field1, field2, field3 FROM table WHERE gid = '
                        + str(gid[0]))
            input_text = cur2.fetchall()
        except Exception as e:
            conn.rollback()
            print ("DB error: ", sys.exc_info()[0], e)
            log_file.write(str(datetime.datetime.today()) 
                           + " DB Error SELECT FLickr id " 
                           + str_gid + " : " 
                           + str(e).replace("\n"," ") + "\n")
            print (str(counter) + " of " + str(number_records)) 
            counter += 1
            continue

        # actual processing of input strings
        # split input up and replace unwanted characters
        for input_element in input_text[0]:
            unigrams = re.split(' |,', str(input_element)) # or any other delimiters
            for unigram in unigrams:
                input_unigrams.append(pattern.sub('', unigram.lower()))
        # create term vector
        for input_unigram in input_unigrams:
            input_unigram_stemmed = stemmer.stem(input_unigram)
            if input_unigram_stemmed in term_dict:
                term_vector[term_dict[input_unigram_stemmed]] += 1
                found_terms.append(input_unigram_stemmed)
        if not found_terms:
            found_terms = '0'
        else:
            found_terms = ','.join(str(s) for s in found_terms)
        
        # insert into table
        data = ('{'+','.join(str(i) for i in term_vector)+'}',
                found_terms, 
                str_gid)

        try:
            cur2.execute("""UPDATE table
                        SET (term_vector, terms_found) =
                        (%s,%s)
                        WHERE gid = (%s);""", data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print ("DB error: ", sys.exc_info()[0], e)
            log_file.write(str(datetime.datetime.today())
                           + " DB Error UPDATE with FLickr id "
                           + str_gid + " : "
                           + str(e).replace("\n"," ") + "\n")

        print (str(counter) + " of " + str(number_records))
        counter += 1

    conn.close()    
    log_file.close()

if __name__=="__main__":
    main() 