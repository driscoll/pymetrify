# -*- coding: utf-8 -*-

import datetime
import json
import multiprocessing
import re


#
# Regex
#

re_mention = re.compile(r'@([A-Za-z0-9_]+)')
re_retweet = re.compile(r'(\"@|RT @|MT @|via @)([A-Za-z0-9_]+)')
re_via = re.compile(r'via @[a-z0-9_]*$')


#
# Helper functions
#

def earlier(dt_a, dt_b):
    """Return the earlier of two datetime objects"""
    if dt_a < dt_b:
        return dt_a
    return dt_b

def later(dt_a, dt_b):
    """Return the later of two datetime objects"""
    if dt_a < dt_b:
        return dt_b
    return dt_a

def from_postedTime(postedTime):
    """Convert date an ISO formatted strings to Python datetime objects
    """
    return datetime.datetime(int(postedTime[:4]),
                             int(postedTime[5:7]),
                             int(postedTime[8:10]),
                             int(postedTime[11:13]),
                             int(postedTime[14:16]),
                             int(postedTime[17:19]))

def get_postedTimeObj(tweet):
    if u'postedTimeObj' in tweet:
        return tweet.get(u'postedTimeObj')
    else:
        return from_postedTime(tweet[u'postedTime'])


#
# Tweet processing functions
#

def parse_mentions(tweet):
    mentions = tweet.get('twitter_entities', {}).get('user_mentions', [])
    parsed = [m for m in re_mention.finditer(tweet.get('body', ''))]
    if len(parsed) > len(mentions):
        starting_indices = [m[u'indices'][0] for m in mentions]
        for mention in parsed:
            if not (mention.start(1)-1) in starting_indices:
                username = mention.group(1)
                u = {
                    u'id_str': '-1',
                    u'indices': [mention.start(1)-1, mention.end(1)],
                    u'screen_name': username
                    }
                mentions.append(u)
    return mentions

def parse_retweet(tweet):
    rt = {}
    if tweet.get(u'verb', u'') == 'share':
        rt = {
            u'edited': False,
            u'retweeted_author_id_str': tweet.get('object', {})
                                             .get('actor', {})
                                             .get('id_str', u''),
            u'retweeted_author_username': tweet.get('object', {})
                                               .get('actor', {})
                                               .get('preferredUsername', u'')
        }
    else:
        m = re_retweet.search(tweet.get(u'body', ''))
        if m:
            retweeted_author_username = m.group(2)
            rt = {
                u'edited': False,
                u'retweeted_author_username': retweeted_author_username,
                u'retweeted_author_id_str': '-1'
            }
    # If RT, is there add'l commentary?
    if rt:
        if tweet[u'verb'] == 'post':
            if not tweet[u'body'][:2].lower() in ('rt', 'mt'):
                if (tweet[u'body'][:2] == '"@'):
                    if not (tweet[u'body'][-1] == '"'):
                        rt[u'edited'] = True
                elif not re_via.search(tweet[u'body'].lower()):
                        rt[u'edited'] = True
    return rt

def parse_hashtags(tweet):
    hashtags = tweet.get('twitter_entities', {}).get('hashtags', [])
    return [ht[u'text'].lower() for ht in hashtags]

def parse_urls(tweet):
    urls = tweet.get('twitter_entities', {}).get('urls', [])
    # TODO These URLs often need to be lengthened further
    return [u[u'expanded_url'] for u in urls]

def extract_tweet_stats(tweet):
    """Transform tweet into a tuple of true/false values
        For use in map-reduce situations
    """
    count = 1

    postedTimeObj = get_postedTimeObj(tweet)

    mentions = parse_mentions(tweet)
    is_mention = 0
    is_reply = 0
    if mentions:
        is_mention = 1
        for mention in mentions:
            if mention[u'indices'][0] == 0:
                is_reply = 1 
                break

    rt = parse_retweet(tweet)
    if rt:
        is_original = 0
        is_retweet = 1
        if rt[u'edited']:
            is_edited_retweet = 1
            is_unedited_retweet = 0
        else:
            is_edited_retweet = 0
            is_unedited_retweet = 1
    else:
        is_original = 1
        is_retweet = 0
        is_edited_retweet = 0
        is_unedited_retweet = 0

    urls = parse_urls(tweet)
    if urls:
        has_url = 1
    else:
        has_url = 0

    hashtags = parse_hashtags(tweet)
    if hashtags:
        has_hashtag = 1
    else:
        has_hashtag = 0

    return (count,
            postedTimeObj,
            postedTimeObj,
            is_mention,
            is_reply,
            is_original,
            is_retweet,
            is_edited_retweet,
            is_unedited_retweet,
            has_url,
            has_hashtag)
    
def reduce_tweet_stats(a, b):
    return (a[0] + b[0],
    earlier(a[1], b[1]),
    later(a[2], b[2]),
    a[3] + b[3],
    a[4] + b[4],
    a[5] + b[5],
    a[6] + b[6],
    a[7] + b[7],
    a[8] + b[8],
    a[9] + b[9],
    a[10] + b[10])

def combine(tweet_stats):
    global stats
    stats = reduce_tweet_stats(stats, tweet_stats)

def process_tweets(inq, outq):
    while True:
        tweet = inq.get()
        if tweet == None:
            break
        outq.put(extract_tweet_stats(tweet))
    outq.put(None)

def get_tweets(q):
    f = open('activity_streams_example.json', 'rb') 
    for l in f:
        q.put(json.loads(l.strip()))
    q.put(None)
    f.close()
    q.close()
    return True

def finalize_stats(q):
    stats = (0,
            datetime.datetime.now(),
            datetime.datetime(2006, 3, 21),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0)
    for s in q.get():
        if s == None:
            break
        stats = reduce_tweet_stats(stats, s)
    return stats


if __name__=="__main__":

    inbox = multiprocessing.Queue()
    outbox = multiprocessing.Queue()
    db_writer = multiprocessing.Process(target=get_tweets, args=(inbox,))
    db_writer.start()
    db_reader = multiprocessing.Process(target=process_tweets, args=(inbox, outbox))
    db_reader.start()
    db_reader.join()
    print finalize_stats(outbox)





