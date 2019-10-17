# kafka-offsets-migrator
Python 3 script to migrate consumer group offsets from one cluster to another

## Overview
Translates consumer group offsets as part of a migration to a new cluster.  Given a consumer group, source and destination cluster, it will:

- find the topics associated with the consumer group
- for each topic partition (TPO), find the consumer group's committed offsets

For each TPO: 

- read the source cluster message, identify the message timestamp and hash the payload
- call OffsetsForTimes() on the destination cluster to find the offset for a message with an equal or greater timestamp
- read the destination cluster message and compare its message payload hash to the source cluster 
- if the message payload hashes are equal, then mark this as a match
- if the hashes don't match then two scenarios are at play:
	1. there were multiple messages produced during the same timestamp (possibly, since timestamps only have millisecond resolution)
	2. the messages is missing - if so, eventually throw an error before migrating *_any_* offsets.


For scenario (1) above, we:

- advance the message timestamp by one millisecond 
- call OffsetsForTimes() again to find the next offset - this becomes the range of offsets it
  will traverse over to find a matching hash.
  - if there were no more recent timestamps on the topic partition, it will call getWatermarkOffsets() to get the last offset and traverse accordingly.
    1. If the number of messages to traverse is large (currently set at 500) it logs a warning.
- iterate over each message, hash the payload and compare to source cluster
  1. if there is a match, break out of the loop and search for the next unmatched message
  2. if there is no match, then the message is considered missing


## Limits, Warnings and shrugging of shoulders

1. We assume that messages in the destination cluster *have not been mutated in any way*:
 . timestamps are the same - there are cluster replication settings that invalidate this requirement, so do check this
 . payload is byte for byte the same as the source - otherwise our hashing will fail
 . not reordered 
 . not repartitioned

2. We assume the source and destination consumer group and topic names are the same.


## Testing
There is a reasonable test suite in place that checks a few things here and there. It uses docker-compose to spin up some Confluent Platform 5.3.0 images:

 . 2x zookeeper
 	  * zookeeper1:20181
 	  * zookeeper2:20182
 . 2x broker
     * broker1:29091
     * broker2:29092

Each broker+zk pair is configured as a separate cluster

You can start your containers prior, or let pytest spin them up. Example of using running containers:

```
  cd tests
  pytest --docker-compose-no-build 
         --use-running-containers 
         --docker-compose=docker-compose.yml 
         --capture=no 
         --prepdata
```

The `--prepdata` option causes pytest to delete and recreate some test topics in each cluster and produce data to each as part of test setup.
You can omit `--prepdata` (once you've run it at least once) and pytest will run all the tests except the test that preps the data, thus saving a little time.

## Running the offset migrator

Set up your folder..

```
python3 -m venv venv  # create a virtual env
source venv/bin/activate
pip install -r requirements.txt
```

Run, like the wind:

```
python offset_translator.py 
         --source-broker localhost:29091 
         --dest-broker localhost:29092 
         --group offset_translator_test
```

You can also optionally provide a `--topic` argument and it will only operate on that one topic, rather tha all topics in the consumer group.

It will log to stdout and also to a local file called `offset_translator.log`.

## Finally
- While I enjoyed writing this, you may not enjoy the results of using it - run away while you can. 
- Not tested on animals, or in production for that matter.
- It would be nice to have a topic regex feature to transform source topics to destination topics
