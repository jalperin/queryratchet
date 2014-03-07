import re, itertools, random
import json
import urllib, urllib2

from xylose.scielodocument import Article
from ratelimited import RateLimited

from time import sleep, strftime, strptime
import datetime

import numpy as np
import matplotlib.pyplot as plt

import cPickle

import ConfigParser
Config = ConfigParser.ConfigParser()
Config.read('config.cnf')

ratelimited = RateLimited(10,1) # rate limit to 3 per second

def fetch_scielo_identifiers():
    base_url = Config.get('scielo_identifier', 'url').strip('/')
    port = ':' + Config.get('scielo_identifier', 'port')
    endpoint = Config.get('scielo_identifier', 'endpoint')
    earliest = strptime(Config.get('ratchet', 'earliest_load'), '%Y-%m-%d')
    
    offset = 0
    
    data =  {'collection': 'scl'}
    i = 0
    while True:
        data['offset'] = offset
        url = base_url + port + endpoint + '?' + urllib.urlencode(data)
        content = json.load(ratelimited.urlopen(url))
    
        offset = content['meta']['offset'] + len(content['objects'])
  
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
            

        # quit when we've reached the end
        if offset > content['meta']['total']:
            break
            
    return content

def fetch_scielo_article(id):
    base_url = Config.get('scielo_article', 'url').strip('/')
    port = ':' + Config.get('scielo_article', 'port')
    endpoint = Config.get('scielo_article', 'endpoint')
    data = {'code': id}
    url = base_url + port + endpoint + '?' + urllib.urlencode(data)
    content = json.load(ratelimited.urlopen(url))

    return Article(content)

def fetch_downloads(id):
    base_url = Config.get('ratchet', 'url').strip('/')
    port = ':' + Config.get('ratchet', 'port')
    endpoint = Config.get('ratchet', 'endpoint')
    
    data = {'code': id}
    url = base_url + port + endpoint + '?' + urllib.urlencode(data)
    print 'fetching: ', id, '...', 
    response = json.load(ratelimited.urlopen(url))
    print 'done'
    if not response:
        return None
    
    # confirms these are all articles
    assert(response[0]['type'] == 'article')
        
    return response[0]   #weirdly returns list instead of straight dict
    
def _make_date_from_ratchet_keys(y,m,d):
    '''
    Ratchet uses yXXXX, mXX, and dXX as strings
    to make a date object we need to strip the first char 
    turn into an int, and pass through date object
    '''
    return datetime.date(int(y[1:]), int(m[1:]), int(d[1:]))

def make_time_series(ratchet_response):
    ''' 
    Makes a Pandas Series relative to Jan 1, 2000 for easy comparisons
    '''
    time0 = datetime.date(2000,1,1)
    
    years = filter(lambda x: re.match('y\d\d\d\d', x), ratchet_response.keys())
    years.sort()
    
    # We can do min because the 'total' key comes after m00 and d00 in the alphabet
    min_year = years[0]
    min_month = min(ratchet_response[min_year].keys())
    min_day = min(ratchet_response[min_year][min_month])
    min_date = _make_date_from_ratchet_keys(min_year, min_month, min_day)
    
    days = []
    accesses = []
    for y in years:
        for m in filter(lambda x: re.match('m\d\d', x), ratchet_response[y].keys()):
            for d in filter(lambda x: re.match('d\d\d', x), ratchet_response[y][m].keys()):
                current_date = _make_date_from_ratchet_keys(y, m, d)
                day = (current_date-min_date).days  # number of days since first access
                days.append(time0+datetime.timedelta(days=day)) # as day relative to time0
                accesses[day] = ratchet_response[y][m][d]

    return accesses
    
def make_plot(accesses):
    colors = itertools.cycle(['b', 'g', 'r', 'c', 'm', 'y', 'k'])
    fig, ax = plt.subplots()
    for series in accesses: 
        print series
        ax.scatter(series.keys(), series.values(), c=next(colors))


    ax.grid(True)
    ax.set_xlim(left=0)
    # ax.set_yscale('log')
    ax.set_xlabel('Days since publication', fontsize=14)
    ax.set_ylabel('Number of downloads', fontsize=14)
    ax.set_title('Number of downloads per day since publication')
    
    
    plt.show()


# content = fetch_scielo_identifiers()
# content = fetch_scielo_article('S1679-39512005000300006')

# accesses = []
# for id in [u'S0074-0276(00)09500214', u'S1516-3180(00)11800106', u'S0036-4665(00)04200103', u'S0036-4665(00)04200108', u'S0102-88392000000200008', u'S0074-0276(00)09500203', u'S0036-4665(00)04200110', u'S0100-72032006000800001', u'S1678-69712011000300006', u'S0074-0276(00)09500118']:
#     downloads = fetch_downloads(id)
#     if downloads:
#         accesses.append(make_time_series(downloads))
#     print id
    
# cPickle.dump(accesses, open('accesses.cPickle', 'wb'))

# pre-loaded the ID's above
# accesses = cPickle.load(open('accesses.cPickle'))
# make_plot(accesses)



