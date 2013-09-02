PyMetrify
=========

PyMetrify is a tool for researchers who are handling small- to medium-sized collections of tweets in Activity Streams format. Inspired by [metrify.awk](http://mappingonlinepublics.net/2012/01/31/more-twitter-metrics-metrify-revisited/), the Metrifier class generates common descriptive statistics and the report() function produces a nice, human-readable report. 

PyMetrify is designed to be easy for beginning programmers to use and modify. Please don't hesitate to get in touch!

## Quick start

Here is a simple script for ingesting tweets from a file and printing a report:
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
$ python get_tweets.py | python pymetrify.py - > output.csv
```

### Output

PyMetrify output is written to stdout. On most systems, you can save the output to a file using the &gt; symbol:
```bash
$ python pymetrify.py tweets.json > output.csv
```

#### Break down report by time period

To report metrics for specific slices of time, __-t__ or __--timeperiod__ followed by a unit of time (second, minute, hour, day, week, month, year):
```bash
$ python pymetrify.py -t hour my_activity_streams_data.json > output.csv
```

#### Group users by activity

To divide users into subgroups based on the volume of their output, use __-p__ or __--percentiles__ followed by a comma-separated list of integers corresponding to the percentiles: 
```bash
$ python pymetrify.py -p 1,9,90 my_activity_streams_data.json > output.csv
```

#### Individual user statistics

To output individual user metrics, use __-u__ or __--includeusers__: 
```bash
$ python pymetrify.py -u my_activity_streams_data.json > output.csv
```

#### Produce a metrify.awk-like report

```bash
$ python pymetrify.py -p 1,9,90 -t 'hour' -u tweets.json > output.csv
```
...is roughly equivalent to...
```bash
$ gawk -F , -f metrify.awk divisions=1,9,90 time="hour" tweets.csv > metrics.csv
```

## Known issues

> "Premature optimization is the root of all evil" -- Donald Knuth, 1974

- [ ] The report() function is very inefficient and not appropriate for large collections. 
- [ ] How should we handle "orphaned" usernames who do not have an id_str?

### Mentions are a generally ambiguous category
* In PyMetrify, "@-mention" and "@-reply" correspond to "@-reply" and "genuine @-reply" in metrify.awk
* Users are identified by a unique numeric ID, but 
* Users can and do change their username as often as they like 
* In rare cases, PyMetrify identifies a username that Twitter's parser missed which means that the user data is assigned to an empty ID

### Retweets are ambiguous if they use "via @username"
* The RT parser starts with pre-parsed RT data via Gnip's use of the Activity Streams "share" verb, then checks with a regex for "MT @username", "RT @username", or "via @username" 
* Distinguishing "edited" from "unedited" retweets is a step in the right direction
* Currently PyMetrify marks "via @username" as a RT but there's a semantic problem -- this may be a RT or a citation to a website, depending on the sending app:

### Other ambiguities

* "Tweets with any URLs/hashtags" are distinguished from "unique urls/hashtags" in the collection (the latter is always less than or equal to the former) 
* "Mentions" are a superset of "retweets" (i.e. a retweet is considered a special type of @-mention)
* If a tweet has multiple @-mentions, the Metrifier counts _each_ one
* "Users" includes all users (even if they only appear in an @-mention or retweet)
* "Authors" is the subset of users who sent one or more outbound tweets

## Acknowledgements

This project began as a port of metrify.awk version 1.2 which was written by [Axel Bruns](http://snurb.info/) and [published in 2012](http://mappingonlinepublics.net/2012/01/31/more-twitter-metrics-metrify-revisited/) on [Mapping Online Publics](http://mappingonlinepublics.net) under a [Creative Commons BY-NC-SA](http://creativecommons.org/licenses/by-nc-sa/2.0/) license.

PyMetrify was written by [Kevin Driscoll](http://kevindriscoll.info/) and published in 2013 under the [GNU General Public License, version 3](http://www.gnu.org/licenses/gpl.html). Following the [advice of Creative Commons](http://creativecommons.org/software), GPLv3 was chosen because it is consistent with the spirit of the original metrify.awk license. Plus, research software should be free for scholars to learn from, teach with, modify, and pass on.

If you use PyMetrify in your academic work, please consider including a citation or footnote consistent with the norms of your discipline.

