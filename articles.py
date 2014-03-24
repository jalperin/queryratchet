import re, itertools, random
import json
import urllib, urllib2

from xylose.scielodocument import Article
from ratelimited import RateLimited

from time import sleep, strftime, strptime
import datetime
import sys, os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import cPickle, shelve

import ConfigParser
Config = ConfigParser.ConfigParser()
Config.read('config.cnf')

# use unbuffered stdout
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

ratelimited = RateLimited(10,1) # rate limit to 3 per second

def fetch_scielo_identifiers(from_file = True):
    '''
    This wil return the scielo identifiers
    either by querying them or by reading them from a file
    '''
    fname = Config.get('files', 'datadir') + 'scielo_ids.cPickle'
    if from_file and os.path.isfile(fname):
        # get what is in the file
        f = open(fname)
        all_ids = cPickle.load(f)
        f.close()

    else:
        # do the fetch from API
        base_url = Config.get('scielo_identifier', 'url').strip('/')
        port = ':' + Config.get('scielo_identifier', 'port')
        endpoint = Config.get('scielo_identifier', 'endpoint')
        earliest = strptime(Config.get('ratchet', 'earliest_load'), '%Y-%m-%d')

        offset = 0

        data =  {'collection': 'scl'}
        i = 0
        all_ids = set()
        while True:
            data['offset'] = offset
            url = base_url + port + endpoint + '?' + urllib.urlencode(data)
            content = json.load(ratelimited.urlopen(url))

            all_ids.update([obj['code'] for obj in content['objects']])
            print "%s/%s - %.2f" % (len(all_ids), content['meta']['total'], (100 * len(all_ids) / float(content['meta']['total'])))
            offset = content['meta']['offset'] + len(content['objects'])

            # quit when we've reached the end
            if offset == content['meta']['total']:
                break

        # write it to file for next time
        f = open(fname, 'wb')
        cPickle.dump(ids, f)
        f.close()


    return all_ids

def fetch_scielo_article(id, from_file = True):
    fname = Config.get('files', 'articlesfile')
    shelf = shelve.open(fname)

    if from_file and os.path.isfile(fname) and id in shelf:
        article = shelf[id]
        print 'from shelf'
    else:
        base_url = Config.get('scielo_article', 'url').strip('/')
        port = ':' + Config.get('scielo_article', 'port')
        endpoint = Config.get('scielo_article', 'endpoint')
        data = {'code': id}
        url = base_url + port + endpoint + '?' + urllib.urlencode(data)
        content = json.load(ratelimited.urlopen(url))
        article = Article(content)
        shelf[id] = article
        print "from API"
    shelf.close()

    return article

def loop_ids():
    # grab each of the id's in the result
    for object in content['objects']:
        id = object['code']
        article = fetch_scielo_article(id)
        if not article:
            continue
        pub_date = article.publication_date
        pub_date = pub_date + (10-len(pub_date))/3*'-01' # pad with -01 for complete publication date
        pub_date = strptime(pub_date, '%Y-%m-%d')

        if pub_date < earliest:
            continue
        i += 1
        print i, strftime('%Y-%m-%d', pub_date)

ids = fetch_scielo_identifiers()
ids = [id.encode('utf8') for id in ids] # shelf module has trouble with unicode
ids2013 = filter(lambda x:x[10:14] == '2013', ids)
for id in ids2013:
    fetch_scielo_article(id)

