import urllib2
from urlparse import urlparse
from collections import defaultdict 
import time

class RateLimited(): 
    def __init__(self, rate, per):
        """
        ensures delays of 'rate' requests per 'per' seconds between calls to urllib2.open
        """
        self.rate = rate
        self.per = per
        self.events = defaultdict(list)    # a dictionary of lists of timestamps

    def urlopen(self, url):
        netloc = urlparse(url).netloc
        now = time.time()

        if len(self.events[netloc]) < self.rate:
            first_event = 0
        else:
            first_event = self.events[netloc].pop(0)

        self.events[netloc].append(now)
        elapsed = now - first_event
        
        # if it hasn't been 'per' secs since first event, wait
        if elapsed < self.rate:
            print 'sleeping for: ', self.rate - elapsed
            time.sleep(self.rate - elapsed)

        try:
            content = urllib2.urlopen(url)
        except:
            raise Exception('UrlOpenFail', url)
            
        return content
        
