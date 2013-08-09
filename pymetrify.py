# -*- coding: utf-8 -*-
"""
PyMetrify

Port of metrify.awk

TODO 

Write a method to output one of the top rows of the awk output
Then make the timeperiods method spit out those rows

If methods don't use self, they should be outside of the Metrifier class

Implement percentiles

Test on larger data set (with share verbs and vias)

Command line options
Conform output to match metrify.awk EXACTLY

networkx or igraph?
    @-mention network (weighted, directional with @-replies being stronger)
    RT-network (weighted, directional)
    user-url 2-mode network
Write up differences between this approach and the original metrify.awk
    We say "mention" and "reply", they say "@-reply" and "genuine @-reply"

Questions:
* Queensland counts RTs as a subset of @-mentions -- non obvious
* Do we count @s in RTs the same as other @s?
* Do we count EACH @ in a tweet or just tweets-containing-an-@?
* Is "users" inclusive of users we observe through RT or @ but who didn't author one of the tweets in the collection?
* We distinguish "tweets with any URLs" from the "unique URLs" in the collection (the latter is always <= the former.) 

Kevin Driscoll, 2013

"""

from collections import Counter, defaultdict
import datetime
import fileinput
import itertools
import json
import re

ISOFORMAT = '%Y-%m-%dT%H:%M:%S.000Z'
SEPARATOR = u','

def from_postedTime(postedTime):
    return datetime.datetime.strptime(postedTime, ISOFORMAT)

class Metrifier:

    re_mention = re.compile(r'@([A-Za-z0-9_]+)')
    re_retweet = re.compile(r'(\"@|RT @|MT @|via @)([A-Za-z0-9_]+)')

    def __init__(self):
        # These default values may seem counterintuitive but
        # When after two tweets are "eaten", they will make more sense
        self.timebounds = {
            u'first' : datetime.datetime.now(), 
            u'last' : datetime.datetime(2006, 3, 21) # Twitter founded
        }
        self.tweet = {} 
        self.frequency = Counter()
        self.user = {}
        self.url = Counter() 
        self.hashtag = Counter()
        self.username = {}
        self.dupes = []
        self.subset = {}

    def lookup_author_id_str(self, username):
        return self.username.get(username.lower(), u'-99')

    def parse_mentions(self, tweet):
        mentions = tweet.get('twitter_entities', {}).get('user_mentions', [])
        parsed = [m for m in self.re_mention.finditer(tweet.get('body', ''))]
        if len(parsed) > len(mentions):
            starting_indices = [m[u'indices'][0] for m in mentions]
            for mention in parsed:
                if not (mention.start(1)-1) in starting_indices:
                    username = mention.group(1)
                    mentions.append({
                                u'id': -1,
                                u'id_str' : self.lookup_author_id_str(username),
                                u'indices' : [mention.start(1)-1, mention.end(1)],
                                u'name' : u'',
                                u'screen_name' : username
                    })
        return mentions

    def parse_urls(self, tweet):
        urls = tweet.get('twitter_entities', {}).get('urls', [])
        # TODO These URLs often need to be lengthened further 
        return [u[u'expanded_url'] for u in urls]

    def parse_hashtags(self, tweet):
        hashtags = tweet.get('twitter_entities', {}).get('hashtags', [])
        return [ht[u'text'].lower() for ht in hashtags]
            
    def parse_retweet(self, tweet):
        rt = {}
        if tweet.get(u'verb', u'') == 'share':
            rt = {
                u'edited' : False,
                u'retweeted_author_id_str' : tweet.get('object', {}).get('actor', {}).get('id_str', u''),
                u'retweeted_author_username' : tweet.get('object', {}).get('actor', {}).get('preferredUsername', u'')
            }
        else:
            m = self.re_retweet.search(tweet.get(u'body', ''))
            if m:
                retweeted_author_username = m.group(2)
                rt = {
                    u'edited' : False,
                    u'retweeted_author_username' : retweeted_author_username,
                    u'retweeted_author_id_str' : self.lookup_author_id_str(retweeted_author_username)
                }
        # If RT, is there add'l commentary? 
        if rt:
            if tweet[u'verb'] == 'post':
                if not tweet[u'body'][:2].lower() in ('rt', 'mt'):
                    if (tweet[u'body'][:2] == '"@'):
                        if not (tweet[u'body'][-1] == '"'):
                            rt[u'edited'] = True
                    elif not re.search(r'via @[a-z0-9_]*$', tweet[u'body'].lower()):
                            rt[u'edited'] = True
        return rt

    def iter(self, key=u'body', start=None, end=None):
        if not start:
            start = datetime.datetime.fromtimestamp(0)
        if not end:
            end = datetime.datetime.now()
        condition = lambda tweet: tweet.get(key, False)
        for tweet in itertools.ifilter(condition, (self.tweet[id_str] for id_str in sorted(self.tweet))):
            if tweet['postedTimeObj'] > end:
                break
            if tweet['postedTimeObj'] >= start:
                yield tweet
        return

    def timeperiods(self, period='hour'):
        periods = {
            'year' : 1,
            'month' : 2,
            'day' : 3,
            'hour' : 4,
            'minute' : 5,
            'second' : 6
        }
        def grouper(tweet):
            return tweet[u'postedTimeObj'].timetuple()[:periods[period]]
        print 'Date/time', 'Tweets', 'Users'
        for key, group in itertools.groupby(self.iter(), key=grouper):
            # datetime requires at least (year, month, day)
            while len(key) < 3:
                key += (1,)
            self.subset[key] = Metrifier()
            for tweet in group:
                self.subset[key].eat(tweet)
            print datetime.datetime(*key),
            print self.subset[key].frequency['tweet'],
            print len(self.subset[key].author)

    def chronological(self):
        for id_str in sorted(self.tweet):
            yield self.tweet[id_str]

    def eat(self, tweet):
        # id_str is our unique key
        id_str = tweet[u'id_str']
        if id_str in self.tweet:
            self.dupes.append(id_str)
        self.tweet[id_str] = dict(tweet)
        self.frequency[u'tweet'] += 1

        # Evaluate the date and time that this tweet was sent 
        postedTimeObj = from_postedTime(tweet[u'postedTime'])
        self.tweet[id_str][u'postedTimeObj'] = postedTimeObj
        if not self.timebounds[u'first']:
            self.timebounds[u'first'] = postedTimeObj
        elif postedTimeObj < self.timebounds[u'first']:
            self.timebounds[u'first'] = postedTimeObj
        if not self.timebounds[u'last']:
            self.timebounds[u'last'] = postedTimeObj
        elif postedTimeObj > self.timebounds[u'last']:
            self.timebounds[u'last'] = postedTimeObj

        # Increment tweet count for this author
        author_id_str = tweet.get(u'actor', {}).get(u'id_str', '-1')
        author_username = tweet.get(u'actor', {}).get(u'preferredUsername', u'')
        self.username[author_username.lower()] = id_str
        if not author_id_str in self.user:
            self.user[author_id_str] = Counter()
        self.user[author_id_str][u'tweet'] += 1
        self.frequency[u'author'] += 1

        # Does the text include one or more @-mentions?
        mentions = self.parse_mentions(tweet)
        if mentions:
            self.frequency[u'is_mention'] += 1
            self.tweet[id_str][u'is_mention'] = True
            for mention in mentions:
                # Placing the following line inside the loop means
                # that we are counting individual @s, 
                # not just tweets containing >= 1 @s
                self.user[author_id_str][u'outbound_mention'] += 1

                self.username[mention[u'screen_name'].lower()] = mention[u'id_str']
                if not mention[u'id_str'] in self.user:
                    self.user[mention[u'id_str']] = Counter()
                self.user[mention[u'id_str']][u'inbound_mention'] += 1

                # Is it an @-reply (not visible to all followers)?
                # (aka, does this mention occur at position 0 in the body?)
                if mention[u'indices'][0] == 0: 
                    self.frequency[u'is_reply'] += 1
                    self.tweet[id_str][u'is_reply'] = True
                    self.user[author_id_str][u'outbound_replies'] += 1
                    self.user[mention[u'id_str']][u'inbound_replies'] += 1

        # Is it a RT?
        rt = self.parse_retweet(tweet)
        if rt:
            self.frequency[u'is_retweet'] += 1
            self.tweet[id_str][u'is_retweet'] = True
            self.user[author_id_str][u'outbound_retweets'] += 1
            self.username[rt[u'retweeted_author_username'].lower()] = rt[u'retweeted_author_id_str']
            if not rt[u'retweeted_author_id_str'] in self.user:
                self.user[rt[u'retweeted_author_id_str']] = Counter()
            self.user[rt[u'retweeted_author_id_str']][u'inbound_retweets'] += 1
            # Is it an "edited" or "unedited" retweet?
            if rt[u'edited']:
                self.frequency[u'is_edited_retweet'] += 1
                self.tweet[id_str][u'is_edited_retweet'] = True
                self.user[author_id_str][u'outbound_edited_retweets'] += 1
                self.user[rt[u'retweeted_author_id_str']][u'inbound_edited_retweets'] += 1
            else:
                self.frequency[u'is_unedited_retweet'] += 1
                self.tweet[id_str][u'is_unedited_retweet'] = True
                self.user[author_id_str][u'outbound_unedited_retweets'] += 1
                self.user[rt[u'retweeted_author_id_str']][u'inbound_unedited_retweets'] += 1
        else:
            self.frequency[u'is_original'] += 1
            self.tweet[id_str][u'is_original'] = True

        # URLs?
        urls = self.parse_urls(tweet)
        if urls:
            self.frequency[u'has_url'] += 1
            self.tweet[id_str][u'has_url'] = True
            for url in urls:
                self.user[author_id_str][u'shared_url'] += 1
                self.url[url] += 1

        # Hashtags?
        hashtags = self.parse_hashtags(tweet)
        if hashtags:
            self.frequency[u'has_hashtag'] += 1
            self.tweet[id_str][u'has_hashtag'] = True
            for hashtag in hashtags:
                self.user[author_id_str][u'used_hashtag'] += 1
                self.hashtag[hashtag] += 1

            
            

def print_time_periods(metrifier, period='hour', separator=SEPARATOR):
    periods = {
        'year' : 1,
        'month' : 2,
        'day' : 3,
        'hour' : 4,
        'minute' : 5,
        'second' : 6
    }
    def grouper(tweet):
        return tweet[u'postedTimeObj'].timetuple()[:periods[period]]

    print SEPARATOR.join(qut_header())

    for key, group in itertools.groupby(metrifier.iter(), key=grouper):
        # datetime requires at least (year, month, day)
        while len(key) < 3:
            key += (1,)
        subset =  Metrifier()
        for tweet in group:
            subset.eat(tweet)
        print SEPARATOR.join(map(unicode, qut_row(subset)))

def qut_period_header():
    """Returns a sequence of strings corresponding to the column headers 
        at the top of the default output from metrify.awk"""
    return (
        u'first',
        u'last',
        u'tweets',
        u'users',
        u'tweets:user',
        u'original tweets:user',
        u'retweets:user',
        u'unedited retweets:user',
        u'edited retweets:user',
        u'genuine @replies:user',
        u'URLs:user',
        u'users:tweets',
        u'original tweets',
        u'genuine @replies',
        u'retweets',
        u'unedited retweets',
        u'edited retweets',
        u'URLs',
        u'% original tweets',
        u'% genuine @replies',
        u'% retweets',
        u'% unedited retweets',
        u'% edited retweets',
        u'% URLs',
        u'number of current users from least active 25% (< 1 tweets)',
        u'% of current users from least active 25% (< 1 tweets)',
        u'number of tweets from least active 25% (< 1 tweets)',
        u'% of tweets from least active 25% (< 1 tweets)',
        u'number of current users from > 25% group (> 0 tweets; 1 of 999 users)',
        u'% of current users from > 25% group (> 0 tweets; 1 of 999 users)',
        u'tweets from > 25% group (> 0 tweets; 1 of 999 users)',
        u'% of tweets from > 25% group (> 0 tweets; 1 of 999 users)',
        u'number of current users from > 50% group (> 0 tweets; 1 of 999 users)',
        u'% of current users from > 50% group (> 0 tweets; 1 of 999 users)',
        u'tweets from > 50% group (> 0 tweets; 1 of 999 users)',
        u'% of tweets from > 50% group (> 0 tweets; 1 of 999 users)',
        u'number of current users from > 75% group (> 0 tweets; 997 of 999 users)',
        u'% of current users from > 75% group (> 0 tweets; 997 of 999 users)',
        u'tweets from > 75% group (> 0 tweets; 997 of 999 users)',
        u'% of tweets from > 75% group (> 0 tweets; 997 of 999 users)'
    )


def qut_period_row(metrifier):
    """Returns a sequence of numbers corresponding to the columns at the top
        of the default output from metrify.awk"""

    if not metrifier.frequency:
        sys.stderr.write('This metrifier has not eaten any tweets.')
        sys.stderr.write('\n')
        return tuple()

    # Tweets collected
    tweets = metrifier.frequency['tweet']

    # Unique users who sent >= 1 tweet
    authors = metrifier.frequency['author']

    # Ratio of tweets to users
    tweets_user = tweets / float(authors)

    # Ratio of original tweets to users
    original_tweets_user = metrifier.frequency['is_original'] / float(authors)

    # Ratio of retweets (any kind) to users
    rt_user = metrifier.frequency['is_retweet'] / float(authors)

    # Ratio of unedited RT to users
    rt_unedited_user = metrifier.frequency['is_unedited_retweet'] / float(authors)

    # Ratio of edited RT to users
    rt_edited_user = metrifier.frequency['is_edited_retweet'] / float(authors)

    # Ratio of @-replies (as opposed to mentions) to users
    replies_user = metrifier.frequency['is_reply'] / float(authors)
    
    # Ratio of URLs to users
    urls_user = metrifier.frequency['has_url'] / float(authors)

    # Ratio of authors who sent >= 1 tweet to tweets
    users_tweets = metrifier.frequency['author'] / float(authors)

    # Original tweets
    original = metrifier.frequency['is_original']

    # @-replies (as opposed to mentions)
    replies = metrifier.frequency['is_reply']

    # Retweets of any kind
    rt = metrifier.frequency['is_retweet']

    # Unedited RTs
    rt_unedited = metrifier.frequency['is_unedited_retweet']
    
    # Edited RTs
    rt_edited = metrifier.frequency['is_edited_retweet']
    
    # Unique URLs
    urls = len(metrifier.url)

    # % original tweets
    original_tweets = original / float(tweets)

    # % genuine @replies
    replies_tweets = replies / float(tweets)

    # % retweets
    rt_tweets = rt / float(tweets)

    # % unedited retweets
    rt_unedited_tweets = rt_unedited / float(tweets)

    # % edited retweets
    rt_edited_tweets = rt_edited / float(tweets)

    # % URLs
    urls_tweets = metrifier.frequency['has_url'] / float(tweets)

    # number of current users from least active 25% (< 1 tweets)
% of current users from least active 25% (< 1 tweets)
number of tweets from least active 25% (< 1 tweets)
% of tweets from least active 25% (< 1 tweets)
number of current users from > 25% group (> 0 tweets; 1 of 999 users)
% of current users from > 25% group (> 0 tweets; 1 of 999 users)
tweets from > 25% group (> 0 tweets; 1 of 999 users)
% of tweets from > 25% group (> 0 tweets; 1 of 999 users)
number of current users from > 50% group (> 0 tweets; 1 of 999 users)
% of current users from > 50% group (> 0 tweets; 1 of 999 users)
tweets from > 50% group (> 0 tweets; 1 of 999 users)
% of tweets from > 50% group (> 0 tweets; 1 of 999 users)
number of current users from > 75% group (> 0 tweets; 997 of 999 users)
% of current users from > 75% group (> 0 tweets; 997 of 999 users)
tweets from > 75% group (> 0 tweets; 997 of 999 users)
% of tweets from > 75% group (> 0 tweets; 997 of 999 users)
"""


    return (
                metrifier.timebounds[u'first'],
                metrifier.timebounds[u'last'],
    			tweets,
    			authors,
    			tweets_user,
    			original_tweets_user,
    			rt_user,
    			rt_unedited_user,
    			rt_edited_user,
    			replies_user,
    			urls_user,
    			users_tweets,
    			original,
    			replies,
    			rt,
    			rt_unedited,
    			rt_edited,
    			urls,
    			original_tweets,
    			replies_tweets,
    			rt_tweets,
    			rt_unedited_tweets,
    			rt_edited_tweets,
    			urls_tweets
           )


if __name__ == "__main__":

    metrifier = Metrifier()

    for line in fileinput.input():
        tweet = json.loads(line)
        metrifier.eat(tweet)

   


