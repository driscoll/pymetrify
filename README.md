PyMetrify
=========

PyMetrify is a metrics tool for researchers who are handling small- to medium-sized collections of tweets. Inspired by [metrify.awk](http://mappingonlinepublics.net/2012/01/31/more-twitter-metrics-metrify-revisited/), PyMetrify calculates common descriptive statistics and includes a set of output functions to produce a human-readable report. PyMetrify is designed to be easy for beginning programmers to use and intermediate programmers to modify. 

## Quick start

Here is a simple script for ingesting tweets and producing a report:
```python
import json
import pymetrify

metrifier = pymetrify.Metrifier()
filename = 'my_tweets.json'

with open(filename, 'rb') as f:
    for line in f:
        tweet = json.loads(line)
        metrifier.eat(tweet)

pymetrify.report(metrifier, 
                 period='hour', 
                 percentiles=(1,9,90), 
                 includeusers=True)
```

## Command-line usage

### Input

PyMetrify reads one tweet at a time in [Activity Streams](http://activitystrea.ms/) format. One short term reason for doing this is that third-party services are increasingly providing data in Activity Streams. One long-term reason is that it will make it easier for future versions of PyMetrify to ingest data from platforms other than Twitter. 

#### From a text file

If your Activity Streams objects are stored in a text file, you can specify this file on the commandline: 
```bash
$ python pymetrify.py tweets.json > output.csv
```

#### From another program

You may also pipe Activity Streams objects to PyMetrify from a script that accesses a database or other data source:
```bash
$ python get_tweets.py | python pymetrify.py -t 'second' -p 1,4,95 - > output.csv
```

#### Activity Streams utilities

If you are not currently storing your data in Activity Streams format, fear not:
* [activitystreams2ytk.py](https://github.com/driscoll/activitystreams2ytk/)

### Output

PyMetrify output is written to stdout. On most UNIX-like systems (including OS X), you can save the output to a file like this:
```bash
$ python pymetrify.py tweets.json > output.csv
```

PyMetrify includes several output functions to generate reports. You can specify these functions from the commandline as well as mix and match them. 

#### Break down report by time period

To report metrics for specific slices of time, __-t__ or __--timeperiod__ followed by a unit of time (second, minute, hour, day, week, month, year):
```bash
$ python -t hour my_activity_streams_data.json > output.csv
```

For each period, this report includes the following counts, ratios, and percentages:
* date/time of the first tweet 
* date/time of the last tweet
* tweets
* users
* tweets:user 
* original tweets:user
* retweets:user
* unedited retweets:user
* edited retweets:user
* genuine @replies:user
* URLs:user
* users:tweets
* original tweets 
* genuine @replies
* retweets
* unedited retweets
* edited retweets 
* URLs
* % original tweets
* % genuine @replies
* % retweets
* % unedited retweets 
* % edited retweets
* % URLs
* number of current users from 90% (0 < tweets <= 1)
* % of current users from 90% (0 < tweets <= 1)
* number of tweets from 90% (0 < tweets <= 1)
* % of tweets from least 90% (0 < tweets <= 1)
* number of current users from 10% (1 < tweets <= 13)
* % of current users from 10% (1 < tweets <= 13)
* number of tweets from 10% (1 < tweets <= 13)
* % of tweets from least 10% (1 < tweets <= 13)

#### Group users by activity

To divide users into subgroups based on the volume of their output, use -p or --percentiles followed by a comma-separated list of integers corresponding to the percentiles: 
```bash
$ python -p 1,9,90 my_activity_streams_data.json > output.csv
```

For each group, this report includes the following counts, ratios, and percentages:
* percentile
* tweets
* tweets:total tweets
* original tweets
* original tweets:tweets
* @replies
* @replies:tweets 
* genuine @replies
* genuine @replies:tweets 
* retweets
* retweets:tweets 
* unedited retweets
* unedited retweets:tweets
* edited retweets 
* edited retweets:tweets
* URLs
* URLs:tweets

#### Individual user statistics

To output individual user metrics, use -u or --includeusers: 
```bash
$ python -u my_activity_streams_data.json > output.csv
```

For each user, this report includes the following counts, ratios, and percentages:
* user
* id_str
* percentile
* tweets
* original tweets
* % original
* outbound @-mentions
* % outbound @-mentions
* outbound @-replies
* % outbound @-replies
* outbound retweets
* % outbound retweets 
* outbound unedited retweets
* % outbound unedited retweets
* outbound edited retweets
* % outbound edited retweets
* tweets with URLs
* % tweets with URLs
* tweets with >= 1 hashtags
* % tweets with >= 1 hashtags
* inbound @-mentions
* inbound @-mentions:outbound tweets
* inbound @-replies
* % of inbound mentions are replies
* inbound @-replies:outbound tweets
* inbound retweets
* inbound retweets:outbound tweets
* inbound unedited retweets
* % inbound unedited retweets 
* inbound unedited retweets:outbound tweets
* inbound edited retweets % inbound edited retweets
* inbound edited retweets:outbound tweets

## Known issues

[ ] The report() function is very inefficient and not appropriate for large collections. 

> "Premature optimization is the root of all evil" -- Donald Knuth, 1974

## Acknowledgements

The project began as a port of metrify.awk version 1.2 which was written by Axel Bruns and [published in 2012](http://mappingonlinepublics.net/2012/01/31/more-twitter-metrics-metrify-revisited/) on [Mapping Online Publics](http://mappingonlinepublics.net) under a [Creative Commons BY-NC-SA](http://creativecommons.org/licenses/by-nc-sa/2.0/) license.

PyMetrify was written by [Kevin Driscoll](http://kevindriscoll.info/) and published in 2013 under the [GNU General Public License, version 3](http://www.gnu.org/licenses/gpl.html). Following the [advice of Creative Commons](http://creativecommons.org/software), GPLv3 was chosen because it is consistent with the spirit of the original metrify.awk license. Plus, research software should be free for scholars to learn from, teach with, modify, and pass on.

If you use PyMetrify in your academic work, please consider including a citation or footnote consistent with the norms of your discipline.

