# -*- coding: utf-8 -*-
"""
pymetrify.py

v.0.1

PyMetrify is a tool for researchers who are handling small- to medium-sized collections of tweets in Activity Streams format. Inspired by Axel Bruns' metrify.awk, the Metrifier class generates common descriptive statistics and the report() function produces a nice, human-readable report in CSV. 

Copyright, Kevin Driscoll, 2013

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from collections import Counter, defaultdict
import argparse
import datetime
import itertools
import json
import math
import operator
import re
import sys

#
# Globals
#

ISOFORMAT = '%Y-%m-%dT%H:%M:%S.000Z'
SEPARATOR = u','
VERBOSE = True 

#
# Helper functions
#

def debug(s):
    """Write s to stderr if the verbose flag is set.
    """
    if VERBOSE:
        sys.stderr.write(s)

def from_postedTime_slow(postedTime):
    """Convert date an ISO formatted strings to Python datetime objects
    """
    return datetime.datetime.strptime(postedTime, ISOFORMAT)

def from_postedTime(postedTime):
    """Convert date an ISO formatted strings to Python datetime objects
    """
    return datetime.datetime(int(postedTime[:4]),
                                int(postedTime[5:7]),
                                int(postedTime[8:10]),
                                int(postedTime[11:13]),
                                int(postedTime[14:16]),
                                int(postedTime[17:19]))

def ratio(n, m):
    """Return ratio of n:m as a float, returns -1 if m == 0
    """
    if not n:
        return 0
    if not m:
        return -1
    return (n / float(m))

def percent(n, m):
    """Return n percent of m as a float, returns -100 if m == 0
    """
    return (100.0 * ratio(n, m))

#
# Main classes
#

class Metrifier:

    re_mention = re.compile(r'@([A-Za-z0-9_]+)')
    re_retweet = re.compile(r'(\"@|RT @|MT @|via @)([A-Za-z0-9_]+)')

    def __init__(self):
        # These default values may seem counterintuitive but
        # After two tweets are "eaten", they will make more sense
        self.timebounds = {
            u'first' : datetime.datetime.now(), 
            u'last' : datetime.datetime(2006, 3, 21) # Twitter founded
        }
        self.tweet = {} 
        self.frequency = Counter()
        self.user = {}
        self.user_tweet = defaultdict(list) 
        self.url = Counter() 
        self.hashtag = Counter()
        self.username = {}
        self.activity = []

    def lookup_user_id_str(self, username):
        return self.username.get(username.lower(), u'')

    def parse_mentions(self, tweet):
        mentions = tweet.get('twitter_entities', {}).get('user_mentions', [])
        parsed = [m for m in self.re_mention.finditer(tweet.get('body', ''))]
        if len(parsed) > len(mentions):
            starting_indices = [m[u'indices'][0] for m in mentions]
            for mention in parsed:
                if not (mention.start(1)-1) in starting_indices:
                    username = mention.group(1)
                    mentions.append({
                                u'id_str' : self.lookup_user_id_str(username),
                                u'indices' : [mention.start(1)-1, mention.end(1)],
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
                    u'retweeted_author_id_str' : self.lookup_user_id_str(retweeted_author_username)
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

    def chronological(self):
        for id_str in sorted(self.tweet):
            yield self.tweet[id_str]

    def itertweets(self, key=u'body', start=None, end=None):
        if not start:
            start = datetime.datetime.fromtimestamp(0)
        if not end:
            end = datetime.datetime.now()
        condition = lambda tweet: tweet.get(key, False)
        for tweet in itertools.ifilter(condition, (self.tweet[id_str] for id_str in sorted(self.tweet))):
            if tweet[u'postedTimeObj'] > end:
                break
            if tweet[u'postedTimeObj'] >= start:
                yield tweet
        return

    def iterusers(self, key=u'id_str', reverse=True, include_inactive=False):
        """Iterate over all the users observed in this collection. 
            Only users that have key in their dict keys will be returned.
        """
        activity = []
        for user in self.user.itervalues():
            if include_inactive:
                if not key in user:
                    activity.append((0, user[u'id_str']))
            else:
                activity.append((user[key], user[u'id_str']))
        for count, id_str in sorted(activity, reverse=reverse):
            yield self.user[id_str]
        return 

    def group_users_by_activity(self, key=u'tweet', reverse=False, include_id_str=False):
        for activity, cohort in itertools.groupby(self.iterusers(key=key, reverse=reverse), lambda u: u.get(key, 0)):
            user = []
            user_count = 0
            for u in cohort:
                if include_id_str:
                    if not u[u'id_str'] in user:
                        user.append(u[u'id_str'])
                user_count += 1
            count = user_count * activity
            per_cent = percent(count, self.frequency.get(key, -1))
            cohort_metrics = {
                u'key' : key,
                u'cohort' : activity,
                u'user_count' : user_count,
                u'count' : count,
                u'percent' : per_cent
            }
            if include_id_str:
                cohort_metrics[u'user'] = user
            yield cohort_metrics

    def group_users_by_percentile(self, divisions=(100,)):

        if not self.frequency:
            raise ValueError, "This self is hungry and has not eaten any tweets."

        # Percentile refers to a list of users sorted from most active to least
        if sum(divisions) > 100:
            raise ValueError, "Second argument must be a sequence of integers that sum to less than or equal to 100"
        elif sum(divisions) < 100:
            divisions += (100 - sum(divisions),)

        divisions = sorted(divisions, reverse=True)
        n = 0
        cohort = Counter() 
        cohort[u'user'] = []
        p = 0
        boundary = int(math.ceil(divisions[p]/100.0 * self.frequency[u'tweet']))
        for group in self.group_users_by_activity(include_id_str=True):
            n += group[u'count']
            cohort[u'activity'] = group.pop('cohort')
            group.pop('key')
            cohort.update(group)
            for user_id_str in group.get(u'user', []):
                if not user_id_str in cohort[u'user']:
                    cohort[u'user'].append(user_id_str)
            if n > boundary:
                tweets = []
                for author_id_str in cohort[u'user']:
                    tweets.extend(self.user_tweet[author_id_str])
                yield divisions[p], cohort, tweets
                n = 0 
                cohort = Counter() 
                cohort[u'user'] = []
                p += 1
                boundary = int(math.ceil(divisions[p]/100.0 * self.frequency[u'tweet']))
        # Sometimes we don't reach the last percentile, so we will combine them
        remaining = reduce(operator.add, divisions[p:])
        tweets = []
        for author_id_str in cohort[u'user']:
            tweets.extend(self.user_tweet[author_id_str])
        yield remaining, cohort, tweets

    def eat(self, tweet):

        # id_str is our unique key
        id_str = tweet[u'id_str']
        if id_str in self.tweet:
            return False

        # Add this tweet to the pile 
        self.tweet[id_str] = dict(tweet) 
        self.frequency[u'tweet'] += 1

        # Evaluate the date and time that this tweet was sent 
        postedTimeObj = tweet.get(u'postedTimeObj', from_postedTime(tweet[u'postedTime']))
        self.tweet[id_str][u'postedTimeObj'] = postedTimeObj
    
        # Test the time bounds
        if postedTimeObj < self.timebounds[u'first']:
            self.timebounds[u'first'] = postedTimeObj
        elif postedTimeObj > self.timebounds[u'last']:
            self.timebounds[u'last'] = postedTimeObj

        # Increment tweet count for this author
        author_id_str = tweet.get(u'actor', {}).get(u'id_str', '-1')
        author_username = tweet.get(u'actor', {}).get(u'preferredUsername', u'').lower()
        self.username[author_username] = author_id_str
        if not author_id_str in self.user:
            self.user[author_id_str] = Counter()
            self.user[author_id_str][u'id_str'] = author_id_str
            self.user[author_id_str][u'username'] = author_username
            self.frequency[u'author'] += 1
        self.user[author_id_str][u'tweet'] += 1
        self.user_tweet[author_id_str].append(id_str)

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

                mention_screen_name = mention[u'screen_name'].lower()
                
                self.username[mention_screen_name] = mention[u'id_str']
                if not mention[u'id_str'] in self.user:
                    self.user[mention[u'id_str']] = Counter()
                    self.user[mention[u'id_str']][u'id_str'] = mention[u'id_str']
                    self.user[mention[u'id_str']][u'username'] = mention_screen_name
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
            retweeted_author_username = rt[u'retweeted_author_username'].lower()
            self.username[retweeted_author_username] = rt[u'retweeted_author_id_str'] 
            if not rt[u'retweeted_author_id_str'] in self.user:
                self.user[rt[u'retweeted_author_id_str']] = Counter()
                self.user[rt[u'retweeted_author_id_str']][u'id_str'] = rt[u'retweeted_author_id_str']
                self.user[rt[u'retweeted_author_id_str']][u'username'] = retweeted_author_username
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
            self.user[author_id_str][u'is_original'] +=1

        # URLs?
        urls = self.parse_urls(tweet)
        if urls:
            self.frequency[u'has_url'] += 1
            self.tweet[id_str][u'has_url'] = True
            for url in urls:
                self.user[author_id_str][u'has_url'] += 1
                self.url[url] += 1

        # Hashtags?
        hashtags = self.parse_hashtags(tweet)
        if hashtags:
            self.frequency[u'has_hashtag'] += 1
            self.tweet[id_str][u'has_hashtag'] = True
            for hashtag in hashtags:
                self.user[author_id_str][u'has_hashtag'] += 1
                self.hashtag[hashtag] += 1

        return True            

##################################################################
#
# OUTPUT functions
#
#

def report(metrifier, period='hour', percentiles=(100,), includeusers=False, separator=SEPARATOR):
    """ Produce output in the style of metrify.awk by Mapping Online Publics
        metrifier: Metrifier() object that has already ingested tweets
        period: a time period with which to group tweets
        percentiles: a sequence of percentiles used to group users by activity (1, 9, 90)
        includeusers: option to skipping calculating per-user stats 
        separator: field separator character

    """
    
    # 
    # Prepare
    # 
    
    if not percentiles:
        division = (100,)

    user_percentiles = [] 
    for percentile, cohort, tweets in sorted(metrifier.group_users_by_percentile(percentiles)):
        user_percentiles.append((percentile, cohort, tweets))
    user_percentiles.reverse()

    #
    # Time period breakdown 
    #

    sys.stdout.write(SEPARATOR.join(report_period_header(metrifier, user_percentiles)))
    sys.stdout.write('\n')
    if period:
        # We can skip calculating multiple periods 
        # if the total collection spans less than 1 period.
        delta = metrifier.timebounds['last'] - metrifier.timebounds['first']
        if period == 'second':
            p = 1
        elif period == 'minute':
            p = 60
        elif period == 'hour':
            p = 3600
        elif period == 'day':
            p = 86400
        elif period == 'month':
            p = 2592000
        elif period == 'year':
            p = 946080000
        if abs(delta.total_seconds()) > p:

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

            for count, (key, group) in enumerate(itertools.groupby(metrifier.itertweets(), key=grouper)):
                # datetime requires at least (year, month, day)
                while len(key) < 3:
                    key += (1,)
                subset =  Metrifier()
                # Here's where we use the actual tweet objects
                for tweet in group:
                    subset.eat(tweet)
                period_label = str(count)
                sys.stdout.write(SEPARATOR.join(map(unicode, report_period_row(subset, user_percentiles, period_label))))
                sys.stdout.write('\n')
                sys.stdout.flush()
    sys.stdout.write(SEPARATOR.join(map(unicode, report_period_row(metrifier, user_percentiles, "total"))))
    sys.stdout.write('\n\n')
    sys.stdout.flush()


    #
    # Percentile breakdown 
    #
    sys.stdout.write(SEPARATOR.join(report_percentile_header()))
    sys.stdout.write('\n')
    if not percentiles == (100,):
        for row in iter_report_percentile_rows(metrifier, user_percentiles):
            sys.stdout.write(SEPARATOR.join(map(unicode, row)))
            sys.stdout.write('\n')
            sys.stdout.flush()
    sys.stdout.write(SEPARATOR.join(map(unicode, report_100_percent_row(metrifier))))
    sys.stdout.write('\n\n')
    sys.stdout.flush()

    # 
    # Individual user statistics 
    #

    if includeusers:
        sys.stdout.write(SEPARATOR.join(report_user_header()))
        sys.stdout.write('\n')
        for row in iter_report_user_rows(metrifier, user_percentiles):
            sys.stdout.write(SEPARATOR.join(map(unicode, row)))
            sys.stdout.write('\n')
            sys.stdout.flush()


def report_user_header():
    return [
            u'user',
            u'id_str',
            u'percentile',
            u'tweets'
            "original tweets",
            "% original",
            "outbound @-mentions",
            "% outbound @-mentions",
            "outbound @-replies",
            "% outbound @-replies",
            "outbound retweets",
            "% outbound retweets",
            "outbound unedited retweets",
            "% outbound unedited retweets",
            "outbound edited retweets",
            "% outbound edited retweets",
            "tweets with URLs",
            "% tweets with URLs",
            "tweets with >= 1 hashtags",
            "% tweets with >= 1 hashtags",
            "inbound @-mentions",
            "inbound @-mentions:outbound tweets",
            "inbound @-replies",
            "% of inbound mentions are replies",
            "inbound @-replies:outbound tweets",
            "inbound retweets",
            "inbound retweets:outbound tweets",
            "inbound unedited retweets",
            "% inbound unedited retweets",
            "inbound unedited retweets:outbound tweets",
            "inbound edited retweets",
            "% inbound edited retweets",
            "inbound edited retweets:outbound tweets"
            ]

def iter_report_user_rows(metrifier, percentiles):
    for percentile, cohort, tweet in reversed(percentiles):
        for id_str in sorted(cohort[u'user']):
            tweets = metrifier.user[id_str].get(u'tweet', 0)
            original_tweets = metrifier.user[id_str].get(u'is_original', 0)
            outbound_mentions = metrifier.user[id_str].get(u'outbound_mention', 0)
            outbound_replies = metrifier.user[id_str].get(u'outbound_replies', 0)
            outbound_retweets = metrifier.user[id_str].get(u'outbound_retweets', 0)
            outbound_unedited_retweets = metrifier.user[id_str].get(u'outbound_unedited_retweets', 0)
            outbound_edited_retweets = metrifier.user[id_str].get(u'outbound_edited_retweets', 0)
            inbound_mentions = metrifier.user[id_str].get(u'inbound_mention', 0)
            inbound_replies = metrifier.user[id_str].get(u'inbound_replies', 0)
            inbound_retweets = metrifier.user[id_str].get(u'inbound_retweets', 0)
            inbound_unedited_retweets = metrifier.user[id_str].get(u'inbound_unedited_retweets', 0)
            inbound_edited_retweets = metrifier.user[id_str].get(u'inbound_edited_retweets', 0)
            has_url = metrifier.user[id_str].get(u'has_url', 0)
            has_hashtag = metrifier.user[id_str].get(u'has_hashtag', 0)
            row = [
                metrifier.user[id_str][u'username'],
                id_str,
                percentile,
                metrifier.user[id_str][u'tweet'],
                original_tweets,
                percent(original_tweets, tweets),
                outbound_mentions,
                percent(outbound_mentions, tweets),
                outbound_replies,
                percent(outbound_replies, tweets),
                outbound_retweets,
                percent(outbound_retweets, tweets),
                outbound_unedited_retweets,
                percent(outbound_unedited_retweets, tweets),
                outbound_edited_retweets,
                percent(outbound_edited_retweets, tweets),
                has_url,
                percent(has_url, tweets),
                has_hashtag,
                percent(has_hashtag, tweets),
                inbound_mentions,
                ratio(inbound_mentions, tweets),
                inbound_replies,
                percent(inbound_replies, inbound_mentions),
                ratio(inbound_replies, tweets),
                inbound_retweets,
                ratio(inbound_retweets, tweets),
                inbound_unedited_retweets,
                percent(inbound_unedited_retweets, inbound_retweets),
                ratio(inbound_unedited_retweets, tweets),
                inbound_edited_retweets,
                percent(inbound_edited_retweets, inbound_retweets),
                ratio(inbound_edited_retweets, tweets)
                ]
            yield row

def report_percentile_row(metrifier, label, total_tweets):
    tweets = metrifier.frequency.get(u'tweet', 0)
    original = metrifier.frequency.get(u'is_original', 0)
    mentions = metrifier.frequency.get(u'is_mention', 0)
    replies = metrifier.frequency.get(u'is_reply', 0)
    retweets = metrifier.frequency.get(u'is_retweet', 0)
    unedited_rt = metrifier.frequency.get(u'is_unedited_retweet', 0)
    edited_rt = metrifier.frequency.get(u'is_edited_retweet', 0)
    has_url = metrifier.frequency.get(u'has_url', 0)
    has_hashtag = metrifier.frequency.get(u'has_hashtag', 0)
    return [
        label,
        tweets, 
        percent(tweets, total_tweets),
        original,
        ratio(original, tweets),
        mentions,
        ratio(mentions, tweets),
        replies,
        ratio(replies, tweets),
        retweets,
        ratio(retweets, tweets),
        unedited_rt,
        ratio(unedited_rt, tweets),
        edited_rt,
        ratio(edited_rt, tweets),
        has_url,
        ratio(has_url, tweets),
        has_hashtag,
        ratio(has_hashtag, tweets)
    ]

def report_100_percent_row(metrifier):
    label = u'All {0} users'.format(metrifier.frequency[u'author'])
    total_tweets = metrifier.frequency.get(u'tweet', 0)
    return report_percentile_row(metrifier, label, total_tweets)

def iter_report_percentile_rows(metrifier, percentiles):
    lower_bound = 0
    total_tweets = metrifier.frequency.get(u'tweet', 0)
    for percentile, cohort, tweets in percentiles:
        subset = Metrifier()
        for tweet_id_str in tweets:
            subset.eat(metrifier.tweet[tweet_id_str])
        label = u'users {0}% ({1} < outbound tweets <= {2}; {3} of {4} users)'.format(
                                                                        percentile, 
                                                                        lower_bound, 
                                                                        cohort[u'activity'],
                                                                        cohort[u'user_count'],
                                                                        len(metrifier.user)
                                                                       )
        lower_bound = cohort[u'activity']
        yield report_percentile_row(subset, label, total_tweets)

def report_percentile_header():
    row = [
        u'percentile',
        u'tweets',
        u'% tweets:total tweets',
        u'original tweets',
        u'original tweets:tweets',
        u'@-mentions',
        u'@-mentions:tweets',
        u'@-replies',
        u'@-replies:tweets',
        u'retweets',
        u'retweets:tweets',
        u'unedited retweets',
        u'unedited retweets:tweets',
        u'edited retweets',
        u'edited retweets:tweets',
        u'URLs',
        u'URLs:tweets',
        u'hashtags',
        u'hashtags:tweets'
    ]
    return row

def report_period_header(metrifier, percentiles):
    """Returns a sequence of strings corresponding to the column headers 
        at the top of the default output from metrify.awk"""
    row = [
        u'period',
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
        u'% URLs'
    ]
    count = 0
    for percentile, cohort, tweet in percentiles:
        row.extend([
            u'number of current users from {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity']),
            u'% of current users from {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity']),
            u'number of tweets from {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity']),
            u'% of tweets from least {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity'])
        ])
        count = cohort[u'activity']
    return row


def report_period_row(metrifier, percentiles, label=''):
    """Returns a sequence of numbers corresponding to the columns at the top
        of the default output from metrify.awk"""

    if not metrifier.frequency:
        raise ValueError, "This metrifier has not eaten any tweets."

    row = [label]

    # Time boundaries
    row.append(metrifier.timebounds['first'].strftime(ISOFORMAT))
    row.append(metrifier.timebounds['last'].strftime(ISOFORMAT))

    # Tweets collected
    tweets = metrifier.frequency[u'tweet']
    row.append(tweets)

    # Unique users who sent >= 1 tweet
    authors = metrifier.frequency[u'author']
    row.append(authors)

    # Ratio of tweets to users
    row.append(ratio(tweets, authors))

    # Ratio of original tweets to users
    row.append(ratio(metrifier.frequency[u'is_original'], authors))

    # Ratio of retweets (any kind) to users
    row.append(ratio(metrifier.frequency[u'is_retweet'], authors))

    # Ratio of unedited RT to users
    row.append(ratio(metrifier.frequency[u'is_unedited_retweet'], authors))

    # Ratio of edited RT to users
    row.append(ratio(metrifier.frequency[u'is_edited_retweet'], authors))

    # Ratio of @-replies (as opposed to mentions) to users
    row.append(ratio(metrifier.frequency[u'is_reply'], authors))
    
    # Ratio of URLs to users
    row.append(ratio(metrifier.frequency[u'has_url'], authors))

    # Ratio of authors who sent >= 1 tweet to tweets
    row.append(ratio(metrifier.frequency[u'author'], authors))

    # Original tweets
    original = metrifier.frequency[u'is_original']
    row.append(original)

    # @-replies (as opposed to mentions)
    replies = metrifier.frequency[u'is_reply']
    row.append(replies)

    # Retweets of any kind
    rt = metrifier.frequency[u'is_retweet']
    row.append(rt)

    # Unedited RTs
    rt_unedited = metrifier.frequency[u'is_unedited_retweet']
    row.append(rt_unedited)

    # Edited RTs
    rt_edited = metrifier.frequency[u'is_edited_retweet']
    row.append(rt_edited)
    
    # Unique URLs
    urls = len(metrifier.url)
    row.append(urls)

    # % original tweets
    row.append(original / float(tweets))

    # % genuine @replies
    row.append(replies / float(tweets))

    # % retweets
    row.append(rt / float(tweets))

    # % unedited retweets
    row.append(rt_unedited / float(tweets))

    # % edited retweets
    row.append(rt_edited / float(tweets))

    # % of tweets with any URLs
    row.append(ratio(metrifier.frequency[u'has_url'], tweets))

    for percentile, cohort, tweets in percentiles:

        # number of current users _% (_ < tweets <= _)
        row.append(cohort[u'user_count'])
        
        # % of current users _% (_ < tweets <= _)
        row.append(percent(cohort[u'user_count'], authors))

        # number of tweets _% (_ < tweets <= _)
        row.append(cohort[u'count'])

        # % of tweets _% (_ < tweets <= _)
        row.append(percent(cohort[u'count'], len(tweets)))

    return row


if __name__ == "__main__":

    class PercentilesAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            percentiles = tuple(int(p) for p in values.split(','))
            if not sum(percentiles) <= 100:
                raise argparse.ArgumentError(self, 'The sum of the percentiles must be less than 100.')
            setattr(namespace, self.dest, percentiles)
   
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--percentiles', help="Report user activity metrics by percentiles, e.g. 90,9,1 (Note: these must sum to less than 100)", action=PercentilesAction)
    parser.add_argument('-t', '--timeperiod', help="Report tweet metrics by time period", choices=['year', 'month', 'day', 'hour', 'minute', 'second'], type=str)
    parser.add_argument('-u', '--includeusers', help="Report descriptive statistics for each user", action="store_true")
    parser.add_argument('-v', '--verbose', help="Verbose output to stderr", action="store_true")
    parser.add_argument('INPUT', help="Source with Activity Streams objects, one per line", default="-", type=argparse.FileType('r'))
    args = parser.parse_args()

    VERBOSE = args.verbose
    
    metrifier = Metrifier()

    for line in args.INPUT:
        tweet = json.loads(line)
        metrifier.eat(tweet)

    report(metrifier, args.timeperiod, args.percentiles, args.includeusers)


   


