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

import ConfigParser
Config = ConfigParser.ConfigParser()
Config.read('config.cnf')

# use unbuffered stdout
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

ratelimited = RateLimited(10,1) # rate limit to 3 per second

def fetch_downloads(id):
    base_url = Config.get('ratchet', 'url').strip('/')
    port = ':' + Config.get('ratchet', 'port')
    endpoint = Config.get('ratchet', 'endpoint')

    data = {'code': id}
    url = base_url + port + endpoint + '?' + urllib.urlencode(data)
    print 'fetching: ', id, '...',
    try:
        response = json.load(ratelimited.urlopen(url))
        print 'OK'
    except Exception as e:
        print 'failed'
        print e.args
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
    return datetime.datetime(int(y[1:]), int(m[1:]), int(d[1:]))

def make_series(id, ratchet_response):
    '''
    Makes a Pandas Series relative to Jan 1, 2000 for easy comparisons
    But keep the first date used and the ID
    returns MultiIndex (days, id, start_date) Downloads
    '''
    print 'making series ...',
    years = filter(lambda x: re.match('y\d\d\d\d', x), ratchet_response.keys())
    years.sort()

    # We can do min because the 'total' key comes after m00 and d00 in the alphabet
    min_year = years[0]
    min_month = min(ratchet_response[min_year].keys())
    min_day = min(ratchet_response[min_year][min_month])
    min_date = _make_date_from_ratchet_keys(min_year, min_month, min_day)

    days = []
    accesses = {}
    for y in years:
        for m in filter(lambda x: re.match('m\d\d', x), ratchet_response[y].keys()):
            for d in filter(lambda x: re.match('d\d\d', x), ratchet_response[y][m].keys()):
                current_date = _make_date_from_ratchet_keys(y, m, d)
                day = (current_date-min_date).days  # number of days since first access
                # days.append(time0+datetime.timedelta(days=day)) # as day relative to time0
                accesses[min_date+datetime.timedelta(days=day)] = ratchet_response[y][m][d]

    s = pd.Series(accesses.values(), index=accesses.keys())
    s = s.sort_index()  # need to sort the series to avoid weirdness when working with time
    start = s.idxmin()  # the first date for which there is data
    df = pd.DataFrame(s)
    df['id'] = str(id) # use str (not unicode) because hdf5 cannot handle unicode
    df['start_date'] = start
    df = df.reset_index()
    df.columns = ['days', 'downloads', 'id', 'start_date']

    # calculate the days since publication
    df['days'] = df.days.map(lambda x:(x - start).days)

    df = df.set_index(['days', 'id', 'start_date'])

    print 'done'
    return df.downloads

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

series = []

offset = 0;
ids_fname = Config.get('files', 'idsfile')
f = open(ids_fname, 'rb')

fname = Config.get('files', 'h5file')

try:
    os.remove(fname) # clear the file before starting (this will probably get me in trouble later)
except:
    pass

# create the store and append, using data_columns where I possibily
# could aggregate
with pd.get_store(fname) as store:
    for l in f:
        id = l.split(',')[0]
        downloads = fetch_downloads(id)
        if downloads:
            store.append('s', make_series(id, downloads), min_itemsize = 50)
        else:
            # this may be slow, but its a better way to make sure
            failure_file = open(Config.get('ratchet', 'failure_file'), 'a')
            failure_file.write(id + "\n")
            failure_file.close()

        offset += 1
        print offset, id


f.close()

# This stuff might be useful for using the data
# s = pd.concat([data['S0036-4665(00)04200103'], data['S0036-4665(00)04200108']])
# s.index = s.index.droplevel('start_date')
# s.loc[s.index.get_level_values('days') < 30].unstack('id')

# make_plot(accesses)