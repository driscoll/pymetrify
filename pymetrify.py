# -*- coding: utf-8 -*-
"""
PyMetrify

Port of metrify.awk

TODO 
Conform output to match metrify.awk EXACTLY
edited/unedited RT
import networkx
    @-mention network (weighted, directional with @-replies being stronger)
    RT-network (weighted, directional)
    user-url 2-mode network
Write up differences between this approach and the original metrify.awk
    We say "mention" and "reply", they say "@-reply" and "genuine @-reply"

Questions:
* Queensland counts RTs as a subset of @-mentions -- non obvious
* Do we count @s in RTs the same as other @s?
* Do we count EACH @ in a tweet or just tweets-containing-an-@?

Kevin Driscoll, 2013

"""

from collections import Counter, defaultdict
import fileinput
import json
import re

class Metrifier:
    
    re_mention = re.compile(r'@([A-Za-z0-9_]+)')
    re_retweet = re.compile(r'(\"@|RT @|MT @|via @)([A-Za-z0-9_]+)')

    def __init__(self):
        self.tweet = {}
        self.author = {}
        self.url = Counter() 
        self.hashtag = Counter()
        self.username = {}
        self.dupes = []

    def digest(self):

        tweet_count = len(self.tweet)
        author_count = len(self.author)
        original_tweet_count = len(filter(lambda t: t['is_original'], self.tweet.itervalues()))
        mention_count = len(filter(lambda t: t['is_mention'], self.tweet.itervalues()))
        reply_count = len(filter(lambda t: t['is_reply'], self.tweet.itervalues()))
        rt_count = len(filter(lambda t: t['is_retweet'], self.tweet.itervalues()))
        tweets_with_links_count = len(filter(lambda t: t['has_url'], self.tweet.itervalues()))
        hashtagged_tweets_count = len(filter(lambda t: t['has_hashtag'], self.tweet.itervalues()))


        print "Total tweets:", tweet_count 
        print
        print "Tweets with >= 1 @-mentions:", mention_count
        print "Replies (tweets start with @-mention):", reply_count
        print "Replies:Mentions", mention_count/float(reply_count)
        print
        print "Original tweets:", original_tweet_count
        print "Original:All tweets:", original_tweet_count/float(tweet_count)
        print
        print "Retweets:", rt_count
        print "Retweets:All tweets:", rt_count/float(tweet_count)
        print
        print "Unique URLs:", len(self.url)
        print "% of tweets with URLs:", tweets_with_links_count/float(tweet_count)
        print "Top 10 URLs:"
        for url, count in self.url.most_common(10):
            print url, count
        print
        print "Unique hashtags:", len(self.hashtag)
        print "% of tweets with hashtasg:", hashtagged_tweets_count/float(tweet_count)
        print "Top 10 hashtags:"
        for hashtag, count in self.hashtag.most_common(10):
            print hashtag, count
        print
        print "Unique authors:", author_count
        print "Tweets:authors:", tweet_count/float(author_count)


    def add_author(self, id_str, username=u''):
        if username:
            self.username[username.lower()] = id_str
        self.author[id_str] = Counter()

    def add_tweet(self, id_str):
        if id_str in self.tweet:
            self.dupes.append(id_str)
        self.tweet[id_str] = defaultdict(bool)

    def lookup_author_id_str(self, username):
        return self.username.get(username.lower(), u'-99')

    def parse_mentions(self, tweet):
        mentions = tweet.get('twitter_entities', {}).get('user_mentions', [])
        parsed = [m for m in self.re_mention.finditer(tweet.get('body', ''))]
        if len(parsed) > len(mentions):
            starting_indices = [m['indices'][0] for m in mentions]
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
        return [u[u'expanded_url'] for u in urls]

    def parse_hashtags(self, tweet):
        hashtags = tweet.get('twitter_entities', {}).get('hashtags', [])
        return [ht['text'].lower() for ht in hashtags]
            
    def parse_retweet(self, tweet):
        if tweet.get(u'verb', u'') == 'share':
            return {
            u'retweeted_author_id_str' : tweet.get('object', {}).get('actor', {}).get('id_str', u''),
            u'retweeted_author_username' : tweet.get('object', {}).get('actor', {}).get('preferredUsername', u'')
            }
        else:
            m = self.re_retweet.search(tweet.get(u'body', ''))
            if m:
                retweeted_author_username = m.group(2)
                return {
                    u'retweeted_author_username' : retweeted_author_username,
                    u'retweeted_author_id_str' : self.lookup_author_id_str(retweeted_author_username)
                }
        return {}

    def eat(self, tweet):
        # id_str is our unique key
        id_str = tweet[u'id_str']
        self.add_tweet(id_str)

        # Increment tweet count for this author
        author_id_str = tweet.get(u'actor', {}).get(u'id_str', '-1')
        author_username = tweet.get(u'actor', {}).get(u'preferredUsername', u'')
        self.add_author(author_id_str, author_username) 
        self.author[author_id_str][u'outbound'] += 1

        # Does the text include one or more @-mentions?
        mentions = self.parse_mentions(tweet)
        if mentions:
            self.tweet[id_str][u'is_mention'] = True
            for mention in mentions:
                # Placing the following line inside the loop means
                # that we are counting individual @s, 
                # not just tweets containing >= 1 @s
                self.author[author_id_str][u'outbound_mention'] += 1
                
                self.add_author(mention[u'id_str'], mention[u'screen_name'])
                self.author[mention[u'id_str']][u'inbound_mention'] += 1

                # Is it an @-reply (not visible to all followers)?
                # (aka, does this mention occur at position 0 in the body?)
                if mention['indices'][0] == 0: 
                    self.tweet[id_str][u'is_reply'] = True
                    self.author[author_id_str][u'outbound_replies'] += 1
                    self.author[mention[u'id_str']][u'inbound_replies'] += 1

        # Is it a RT?
        rt = self.parse_retweet(tweet)
        if rt:
            self.tweet[id_str]['is_retweet'] = True
            self.author[author_id_str][u'outbound_retweets'] += 1
            self.add_author(rt[u'retweeted_author_id_str'], rt[u'retweeted_author_username'])
            self.author[rt[u'retweeted_author_id_str']][u'inbound_retweets'] += 1
        else:
            self.tweet[id_str]['is_original'] = True

        # URLs?
        urls = self.parse_urls(tweet)
        if urls:
            self.tweet[id_str]['has_url'] = True
            for url in urls:
                self.author[author_id_str][u'shared_url'] += 1
                self.url[url] += 1

        # Hashtags?
        hashtags = self.parse_hashtags(tweet)
        if hashtags:
            self.tweet[id_str]['has_hashtag'] = True
            for hashtag in hashtags:
                self.author[author_id_str][u'used_hashtag'] += 1
                self.hashtag[hashtag] += 1

            
            

            



if __name__ == "__main__":

    metrifier = Metrifier()

    for line in fileinput.input():
        tweet = json.loads(line)
        metrifier.eat(tweet)

    metrifier.digest()




