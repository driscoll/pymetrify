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

Adjust singular/plural key names

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

Percentiles are ambiguous
Here's how I am doing it:
For each percentile supplied by the user, calculate a number of tweets (e.g. for 1000 tweets, if percentiles are (90, 10), then tweet breakdown will be (900, 100))
Group all users according to the number of tweets they sent
Sum all of the tweets sent by each group
Starting with the least-active group, add groups to a cohort until their collective tweets exceeds the number for this percentile



Kevin Driscoll, 2013

"""

from collections import Counter, defaultdict
import datetime
import fileinput
import itertools
import json
import math
import operator
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
        self.activity = []
        self.dupes = []

    def lookup_user_id_str(self, username):
        return self.username.get(username.lower(), None)

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
            percent = 100*count/float(self.frequency.get(key, -1))
            cohort_metrics = {
                u'key' : key,
                u'cohort' : activity,
                u'user_count' : user_count,
                u'count' : count,
                u'percent' : percent
            }
            if include_id_str:
                cohort_metrics[u'user'] = user
            yield cohort_metrics

    def iterusers(self, key=u'id_str', reverse=True, include_inactive=False):
        """Iterate over all the users observed in this collection. 
            Only users that have key in their dict keys will be returned.
        """
        if include_inactive:
            activity = list((user.get(key, 0), user[u'id_str']) for user in sorted(self.user.itervalues()))
        else:
            activity = list((user.get(key, 0), user[u'id_str']) for user in sorted(self.user.itervalues()) if key in user)
        activity = sorted(activity, reverse=reverse)
        for count, id_str in activity:
            yield self.user[id_str]
        return 

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
        author_username = tweet.get(u'actor', {}).get(u'preferredUsername', u'').lower()
        self.username[author_username] = author_id_str
        if not author_id_str in self.user:
            self.user[author_id_str] = Counter()
            self.user[author_id_str][u'id_str'] = author_id_str
            self.user[author_id_str][u'username'] = author_username
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
                self.frequency[u'outbound_mention'] += 1

                mention_screen_name = mention[u'screen_name'].lower()
                self.username[mention_screen_name] = mention[u'id_str']
                if not mention[u'id_str'] in self.user:
                    self.user[mention[u'id_str']] = Counter()
                    self.user[mention[u'id_str']][u'id_str'] = mention[u'id_str']
                    self.user[mention[u'id_str']][u'username'] = mention_screen_name
                self.user[mention[u'id_str']][u'inbound_mention'] += 1
                self.frequency[u'inbound_mention'] += 1

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
                self.user[rt[u'retweeted_author_id_str']] = rt[u'retweeted_author_id_str']
                self.user[rt[u'retweeted_author_username']] = retweeted_author_username
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

            
def group_users_by_percentile(metrifier, percentiles=(1, 9, 90), include_id_str=False):

    if not metrifier.frequency:
        raise ValueError, "This metrifier is hungry and has not eaten any tweets."

    # Percentile refers to a list of users sorted from most active to least
    if sum(percentiles) > 100:
        raise ValueError, "Second argument must be a sequence of integers that sum to less than or equal to 100"
    elif sum(percentiles) < 100:
        percentiles += (100 - sum(percentiles),)

    percentiles = sorted(percentiles, reverse=True)
    n = 0
    cohort = Counter() 
    cohort[u'user'] = []
    p = 0
    boundary = int(math.ceil(percentiles[p]/100.0 * metrifier.frequency[u'tweet']))
    for group in metrifier.group_users_by_activity(include_id_str=include_id_str):
        n += group[u'count']
        cohort[u'activity'] = group.pop('cohort')
        group.pop('key')
        cohort.update(group)
        if include_id_str:
            for user_id_str in group.get(u'user', []):
                if not user_id_str in cohort[u'user']:
                    cohort[u'user'].append(user_id_str)
        if n > boundary:
            yield percentiles[p], cohort
            n = 0 
            cohort = Counter() 
            cohort[u'user'] = []
            p += 1
            boundary = int(math.ceil(percentiles[p]/100.0 * metrifier.frequency[u'tweet']))
    # Sometimes we don't reach the last percentile, so we will combine them
    remaining = reduce(operator.add, percentiles[p:])
    yield remaining, cohort


# OUTPUT functions
# TODO move this to separate file eventually

def mop(metrifier, period='hour', percentiles=(1,9,90), separator=SEPARATOR, skipusers=False):

    output = []

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

    print SEPARATOR.join(mop_period_header(metrifier, percentiles))

    for key, group in itertools.groupby(metrifier.itertweets(), key=grouper):
        # datetime requires at least (year, month, day)
        while len(key) < 3:
            key += (1,)
        subset =  Metrifier()
        for tweet in group:
            subset.eat(tweet)
        print SEPARATOR.join(map(unicode, mop_period_row(subset, percentiles)))

    print
    print SEPARATOR.join(mop_percentile_header())

    for row in iter_mop_percentile_rows(metrifier, percentiles):
        print SEPARATOR.join(map(unicode, row))
    print SEPARATOR.join(map(unicode, mop_100_percent_row(metrifier)))

    print
    print SEPARATOR.join(mop_user_header(skipusers))

    for row in iter_mop_user_rows(metrifier, percentiles, skipusers):
        print SEPARATOR.join(map(unicode, row))

def mop_user_header(skipusers=False):

    row = [
        u'user',
        u'id_str',
        u'percentile',
        u'tweets'
    ]
    if not skipusers:
        row.extend([
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
        ])
    return row

def iter_mop_user_rows(metrifier, percentiles, skipusers=False):
    def ratio(s, n):
        if not s:
            return 0
        if not n:
            return -1
        return (100 * s / float(n))
    def percent(s, n):
        if not s:
            return 0
        if not n:
            return -1
        return (100 * s / float(n))
    for percentile, cohort in sorted(list(group_users_by_percentile(metrifier, percentiles, include_id_str=True))):
        for id_str in sorted(cohort[u'user']):
            row = [
                metrifier.user[id_str][u'username'],
                id_str,
                percentile,
                metrifier.user[id_str][u'tweet']
            ]
            if not skipusers:
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
                row.extend([
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
                    ratio(inbound_edited_retweets, tweets),
                ])
            yield row

def mop_100_percent_row(metrifier):
    row = [
        u'All {0} users'.format(metrifier.frequency[u'author']),
        metrifier.frequency[u'tweet'],
        1,
        metrifier.frequency[u'is_original'],
        100*metrifier.frequency[u'is_original']/float(metrifier.frequency[u'tweet']),
        metrifier.frequency[u'is_mention'],
        100*metrifier.frequency[u'is_mention']/float(metrifier.frequency[u'tweet']),
        metrifier.frequency[u'is_reply'],
        100*metrifier.frequency[u'is_reply']/float(metrifier.frequency[u'tweet']),
        metrifier.frequency[u'is_retweet'],
        100*metrifier.frequency[u'is_retweet']/float(metrifier.frequency[u'tweet']),
        metrifier.frequency[u'is_unedited_retweet'],
        100*metrifier.frequency[u'is_unedited_retweet']/float(metrifier.frequency[u'tweet']),
        metrifier.frequency[u'is_edited_retweet'],
        100*metrifier.frequency[u'is_edited_retweet']/float(metrifier.frequency[u'tweet']),
        metrifier.frequency[u'has_url'],
        100*metrifier.frequency[u'has_url']/float(metrifier.frequency[u'tweet'])
    ]
    return row

def iter_mop_percentile_rows(metrifier, percentiles):
    count = 0
    for percentile, cohort in sorted(list(group_users_by_percentile(metrifier, percentiles)), reverse=True):
        row = [
            u'users {0}% ({1} < tweets <= {2}; {3} of {4} users)'.format(
                                                            percentile, 
                                                            count, 
                                                            cohort[u'activity'],
                                                            cohort[u'user_count'],
                                                            len(metrifier.user)
                                                           ),
            cohort[u'count'],
            100*cohort[u'count']/float(metrifier.frequency['tweet'])
        ]
        # original tweets
        row.append(-1)
        # original tweets:tweets
        row.append(-1)
        # @replies
        row.append(-1)
        # @replies:tweets
        row.append(-1)
        # genuine @replies
        row.append(-1)
        # genuine @replies:tweets
        row.append(-1)
        # retweets
        row.append(-1)
        # retweets:tweets
        row.append(-1)
        # unedited retweets
        row.append(-1)
        # unedited retweets:tweets
        row.append(-1)
        # edited retweets
        row.append(-1)
        # edited retweets:tweets
        row.append(-1)
        # URLs
        row.append(-1)
        # URLs:tweets
        row.append(-1)
        yield row
        count = cohort[u'activity']


def mop_percentile_header():
    row = [
        u'percentile',
        u'tweets',
        u'tweets:total tweets',
        u'original tweets',
        u'original tweets:tweets',
        u'@replies',
        u'@replies:tweets',
        u'genuine @replies',
        u'genuine @replies:tweets',
        u'retweets',
        u'retweets:tweets',
        u'unedited retweets',
        u'unedited retweets:tweets',
        u'edited retweets',
        u'edited retweets:tweets',
        u'URLs',
        u'URLs:tweets'
    ]
    return row

def mop_period_header(metrifier, percentiles):
    """Returns a sequence of strings corresponding to the column headers 
        at the top of the default output from metrify.awk"""
    row = [
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
    for percentile, cohort in sorted(list(group_users_by_percentile(metrifier, percentiles)), reverse=True):
        row.extend([
            u'number of current users from {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity']),
            u'% of current users from {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity']),
            u'number of tweets from {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity']),
            u'% of tweets from least {0}% ({1} < tweets <= {2})'.format(percentile, count, cohort[u'activity'])
        ])
        count = cohort[u'activity']
    return row


def mop_period_row(metrifier, percentiles):
    """Returns a sequence of numbers corresponding to the columns at the top
        of the default output from metrify.awk"""

    if not metrifier.frequency:
        raise ValueError, "This metrifier has not eaten any tweets."

    row = []

    # Time boundaries
    row.append(metrifier.timebounds['first'])
    row.append(metrifier.timebounds['last'])

    # Tweets collected
    tweets = metrifier.frequency[u'tweet']
    row.append(tweets)

    # Unique users who sent >= 1 tweet
    authors = metrifier.frequency[u'author']
    row.append(authors)

    # Ratio of tweets to users
    row.append(tweets / float(authors))

    # Ratio of original tweets to users
    row.append(metrifier.frequency[u'is_original'] / float(authors))

    # Ratio of retweets (any kind) to users
    row.append(metrifier.frequency[u'is_retweet'] / float(authors))

    # Ratio of unedited RT to users
    row.append(metrifier.frequency[u'is_unedited_retweet'] / float(authors))

    # Ratio of edited RT to users
    row.append(metrifier.frequency[u'is_edited_retweet'] / float(authors))

    # Ratio of @-replies (as opposed to mentions) to users
    row.append(metrifier.frequency[u'is_reply'] / float(authors))
    
    # Ratio of URLs to users
    row.append(metrifier.frequency[u'has_url'] / float(authors))

    # Ratio of authors who sent >= 1 tweet to tweets
    row.append(metrifier.frequency[u'author'] / float(authors))

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
    row.append(metrifier.frequency[u'has_url'] / float(tweets))

    for percentile, cohort in sorted(list(group_users_by_percentile(metrifier, percentiles))):
        # number of current users _% (_ < tweets <= _)
        row.append(cohort[u'user_count'])
        
        # % of current users _% (_ < tweets <= _)
        row.append(100*cohort[u'user_count']/float(authors))

        # number of tweets _% (_ < tweets <= _)
        row.append(cohort[u'count'])

        # % of tweets _% (_ < tweets <= _)
        row.append(100*cohort[u'count']/float(tweets))

    return row


if __name__ == "__main__":

    metrifier = Metrifier()

    for line in fileinput.input():
        tweet = json.loads(line)
        metrifier.eat(tweet)

    mop(metrifier)


   


