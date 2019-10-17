import sys
import time
import pprint
import logging
import argparse
import hashlib
import subprocess
from collections import defaultdict
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

class OffsetTranslator():
  """Translates consumer group offsets as part of a migration to a new cluster.
  Given a consumer group, source and destination cluster, it will find the topics
  involved in the consumer group and the committed offsets.
  For there it uses OffsetsForTimes() to find the offset for a message with an equal
  or greater time in the destination cluster and compares a hash of the message value
  to confirm if the offset relates to the same message. If not, it advances the timestamp
  by one millisecond and finds the next offset - this becomes the range of offsets it
  will traverse over to find a matching hash.
  If there were no more recent timestamps on the topic partition, it will call
  getWatermarkOffsets() to get the last offset and traverse accordingly.
  If the number of messages to traverse is stupidly large (currently set at 500) it throws
  a warning.
  There is every possibility that the message simply doesn't exist, in which case 
  it will throw an exception.
  """

  def __init__(self, src_bootstrap_server, 
                     src_group_id, 
                     src_topic,
                     dest_bootstrap_server, 
                     dest_group_id):

    self._admin = AdminClient(
        { "bootstrap.servers": src_bootstrap_server })

    # For reading offsets/messages in the source cluster
    self._consumer = Consumer({
            "bootstrap.servers": src_bootstrap_server,
            "group.id": src_group_id,
            "enable.auto.commit": "false"})

    # For reading offsets/messages in the destination cluster
    self._dest_consumer = Consumer({
            "bootstrap.servers": dest_bootstrap_server,
            "group.id": dest_group_id,
            "enable.auto.commit": "false"})

    # Handy instance variables
    self._src_group_id = src_group_id
    self._src_topic = src_topic
    self._src_bootstrap_servers = src_bootstrap_server
    self._dest_group_id = dest_group_id
    self._dest_bootstrap_servers = dest_bootstrap_server
    self._metadata = defaultdict(dict)

    self.logger = logging.getLogger('translator')
    self.logger.info("Offset Translator object instantiated.")
    self.logger.info(f"  Source bootstrap servers: {self._src_bootstrap_servers}")
    self.logger.info(f"  Destination  bootstrap servers: {self._src_bootstrap_servers}")
    self.logger.info(f"  Consumer group: {self._src_group_id}")

  def metadataKeyFromTPO(self, tpo):
    """Return a string key from TopicPartition object for use in metadata hash
    """
    return f"{tpo.topic}::{tpo.partition}"


  def buildMetadataMap(self, tpos):
    """Use TopicPartition data to build internal metadata hash for comparing offsets, timestamps etc between source
       and destination clusters.
    """

    self.logger.info(f"Building metadata map...")

    for tpo in tpos:
        key = self.metadataKeyFromTPO(tpo)
        self._metadata[key] = {
            "src_offset": tpo.offset,
            "src_timestamp": 0,
            "src_hash": None,
            "src_tpo": tpo,
            "src_message": None,
            "dest_offset": None,
            "dest_timestamp": None,
            "dest_hash": None,
            "dest_tpo": None,
            "dest_message": None
        }

    self.logger.info(f"Built metadata for {len(tpos)} TPOs")
    return self._metadata


  def getTPOs(self, topics):
    """Use the AdminAPI to return a list of TopicParition objects for a list of topics
    """

    self.logger.info(f"Getting TPOs for {len(topics)} topics via admin API...")
    tpos = []
    for t in topics:
      for p in  self._admin.list_topics(t).topics[t].partitions:
        tpos.append(TopicPartition(t, p))

    self.logger.info(f"Found {len(tpos)} TPOs for {len(topics)} topics.")
    return tpos


  def updateMetadata(self, metadata):
    """Takes output of inspectTPOMessages() and updates metadata.
    We don't do this automatically within inspectTPOMessagse, as we may want 
    to use inspectTPOMessages on the destination cluster and compare to the 
    source, so updating the object's metadata would render that useless.
    """

    self.logger.info("Updating metadata...")
    for key in metadata.keys():
      for inner_key in metadata[key]:
        self._metadata[key][inner_key] = metadata[key][inner_key] 
      
    # Grab the first key and check if it relates to src_ or dest_ data..
    sample = metadata[next(iter(metadata.keys()))]
    if 'src_offset' in sample.keys():
      cluster = "source"
    elif 'dest_offset' in sample.keys():
      cluster = "destination"
    else:
      raise Exception("Metadata doesn't clearly indicate which cluster it is from.. no src_offset or dest_offset key present...")

    self.logger.info(f"{len(metadata)} updates to metadata from {cluster} cluster.")
    return self._metadata


  def inspectTPOMessages(self, tpos, cluster="source"):
      """ Given a list of TopicPartition objects, for each partition read the message at the
      required offset and extract the timestamp, hash the message value
      """

      self.logger.info(f"Inspecting {len(tpos)} TPOs in {cluster} cluster.")

      # Default to the source cluster consumer; we will also use this
      # to inspect destination cluster messages
      if cluster == "source":
        consumer = self._consumer
      elif cluster == "destination":
        consumer = self._dest_consumer
      else:
        raise Exception("cluster argument to inspectTPOMessages must be one of 'source' or 'destination'")
      
      
      circuit_breaker_retry_count = 0

      metadata = defaultdict(dict)

      # This seems a slow way to just read one message at a time from a partition, but I'm not aware
      # of a better way of reading a single message for each partition when there may be further messages
      # on the partition.
      for tpo in tpos:
        
        # If the tpo.offset is < 0, then the consumer hasn't read anything
        # from the topic partition, so skip it.
        if tpo.offset < 0:
            continue

        consumer.assign([tpo])

        while True:
          # Poll for data on this specific TopicPartition
          m = consumer.poll(1)
          if m is None:
              circuit_breaker_retry_count += 1
              if circuit_breaker_retry_count > 10:
                print("Too many iterations polling for data and getting nothing.")
                break
              else:
                continue
          elif m.error() is None:
            # We'll build a local copy of metadata
            md = {}
            if cluster == "source":
               md['src_offset'] = m.offset()
               md['src_timestamp'] = m.timestamp()[1]
               md['src_hash'] = self.sha256Object(m.value())
               md['src_tpo'] = tpo
               md['src_message'] = m
            elif cluster == "destination":
               md['dest_offset'] = m.offset()
               md['dest_timestamp'] = m.timestamp()[1]
               md['dest_hash'] = self.sha256Object(m.value())
               md['dest_tpo'] = tpo
               md['dest_message'] = m
  
            key = self.metadataKeyFromTPO(tpo)
            metadata[key] = md
            circruit_breaker_retry_count = 0

            # Break the while loop, we've got our data for this topic/partition
            break
          else:
            raise Exception(f"Error reading offset {tpo.offset} from {tpo.topic}/{tpo.partition}: {m.error()}")

      self.logger.info(f"Returning metadata for {len(metadata)} TPOs")
      return metadata


  def sha256Object(self, obj):
    """Return the sha256 digest for a supplied object"""
    return hashlib.sha256(bytes(obj)).hexdigest()

  def getTPOsByTime(self, metadata=None):
    """ Build a list of TopicPartitions using message timestamps instead of offsets
    """
    
    if metadata is None:
      metadata = self._metadata

    self.logger.info(f"Getting offsets from timestamps for {len(metadata)} metadata entries..")

    tpos_by_time = list()
    for key in metadata.keys():
      md = self._metadata[key]
      if md['src_timestamp'] > 0:
        tpo = md['src_tpo']
        tpos_by_time.append(TopicPartition(tpo.topic, tpo.partition, md['src_timestamp']))
        
    # This returns the earliest offset for a given timestamp
    tpos = self._dest_consumer.offsets_for_times(tpos_by_time)

    # Check for errors
    for t in [t for t in tpos if t.error is not None]:
      raise Exception(f"Error getting offset from timestamp: Topic {t.topic}, Partition {t.partition}, Offset {t.offset}: Error {t.error}")

    self.logger.info(f"Returning {len(tpos)} offsets from destination cluster.")
    return tpos


  def findMatchingMessages(self):
    """Iterate over metadata and find matching source/destination messages and
    separate into matched / unmatched buckets, returning a tuple
    """

    self.logger.info("Searching for destination messages that match via message hash...")

    # Iterate over the source cluster metadata and compare to destination cluster
    translated_offsets = list()
    unmatched_offsets = list()

    for key in self._metadata.keys():
      metadata       = self._metadata[key]
      src_tpo        = metadata['src_tpo']
      dest_message   = metadata['dest_message']
      dest_timestamp = metadata['dest_timestamp']
      dest_tpo       = metadata['dest_tpo']

      self.logger.info(f"  Working with TopicPartition({src_tpo.topic},{src_tpo.partition},{src_tpo.offset}) @ {metadata['src_timestamp']}")

      # We found the destination cluster message by offsets_for_times and compared hashes
      # If they match, then the destination offset
      if metadata['src_hash'] == metadata['dest_hash']:
          self.logger.info(f"   FOUND:      TopicPartition({dest_tpo.topic},{dest_tpo.partition},{dest_tpo.offset}) @ {dest_timestamp} in destination cluster")
          self._metadata[key]['matched'] = True
          translated_offsets.append(dest_tpo)
      else:
          self.logger.info(f"   NOT FOUND:  TopicPartition({dest_tpo.topic},{dest_tpo.partition},{dest_tpo.offset}) @ {dest_timestamp} does not have same hash.")
          self.logger.info(f"   will traverse messages and attempt to find a match.")
          self._metadata[key]['matched'] = False
          unmatched_offsets.append(metadata)

    self.logger.info(f"Found {len(translated_offsets)} matching offsets and {len(unmatched_offsets)} that don't match.")
    return (translated_offsets, unmatched_offsets)
      

  def findOffsetRangeToScan(self, md):
    """Using a metadata record as a base, identify how many records (maximum) to scan through to find a match

    We are here because we didn't find a match for source cluster timestamp, which means it is either not there, or 
    multiple messages were produced during that millisecond and our offsets_for_times() call provided the lowest offset 
    for that millisecond.  We will add 1 ms to the timestamp and get the offset (if possible) and then iterate over 
    each message and compare hashes to determine what the exact offset should be.
    """

    self.logger.info("Find the start/end offsets to iterate over to find a match based on message value hash.")

    timestamp_end = md['src_timestamp'] + 1 # add one millisecond 
    tpo = md['dest_tpo']
    starting_offset = md['dest_offset']

    end_offset = self._dest_consumer.offsets_for_times([TopicPartition(tpo.topic, tpo.partition, timestamp_end)])
    self.logger.info(f"Shifting timestamp by 1ms, from {md['src_timestamp']} to {timestamp_end}")
    self.logger.info(f"                           yields an offset of {end_offset[0]}")
   
    target_offset = -1
    if end_offset[0].offset == -1:
      # There are no more recent timestamps for the topic/partition
      # Set the ending offset at the end of partition
      low, high = self._dest_consumer.get_watermark_offsets(TopicPartition(tpo.topic,tpo.partition))
      target_offset = high
      self.logger.info(f"Reading to end of the partition... {target_offset}")
      if target_offset - tpo.offset > 500:
        self.logger.warning(f"    Note: that involves reading and hashing {target_offset - tpo.offet} messages.. might take some time.")
    else:
      # There was a more recent timestamped message, so we'll use that as our target offset
      target_offset = end_offset[0].offset

    self.logger.info(f"Starting offset for scan is {starting_offset} (inclusive)")
    self.logger.info(f"Ending   offset for scan is {target_offset} (exclusive)")

    return (starting_offset, target_offset)


  def compareOffsets(self):
    """For the list of tpos in the source cluster, look them up in the destination
    and compare value hashes; if they match all good; if not, iterate over records
    until a match is found (where duration is one millisecond, based on the 
    assumption that multiple messages have been produced during the same millisecond)
    """

    self.logger.info("Comparing offsets between source and destination cluster...")

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(self._metadata)

    # Check that we have destination cluster offsets and hashes before proceeding - if not, we 
    # have incomplete data and should explode into a ball of flames to the sound of a distorted
    # sitar being played backwards.
    counter = 0
    for k in self._metadata.keys():
      if self._metadata[k]['dest_hash'] is None or    \
         self._metadata[k]['dest_offset'] is None or  \
         self._metadata[k]['src_hash'] is None:   
        counter += 1

    if counter > 0:
      raise Exception(f"{counter} out of {len(self._metadata)} topic partitions have insufficient data. Exiting.")
      

    translated_offsets, unmatched_offsets = self.findMatchingMessages()

    self.logger.info("Working on unmatched offsets...")

    messages_found = 0
    for md in unmatched_offsets:
      tpo = md['dest_tpo']
      (starting_offset, target_offset) = self.findOffsetRangeToScan(md)

      for offset in range(starting_offset, target_offset):
        self.logger.info(f"Inspecting destination cluster message at offset {offset}...")
        results = self.inspectTPOMessages([TopicPartition(tpo.topic, tpo.partition, offset)], cluster="destination")
        if len(results) == 0:
          raise Exception("Didn't get any metadata from call to inspectTPOMessages(). This implies we read data from the source cluster, but couldn't inspect any messages in the destination cluster. Stopping.")
        elif len(results) > 1:
          raise Exception(f"Expecting only one result from call to inspectTPOMessages, but got {len(results)}. Stopping")
        else:
          # Get the (only) key from the dict
          key = next(iter(results))
          dest_hash    = results[key]['dest_hash']
          dest_tpo     = results[key]['dest_tpo']
          dest_message = results[key]['dest_message']

          if dest_hash == md['src_hash']:
            self.logger.info("   FOUND matching record: ")
            self.logger.info(f"                         source hash was {md['src_hash']}, and")
            self.logger.info(f"                         dest_hash is    {dest_hash}")
            self.logger.info(f".                        destination     {dest_tpo}")
            self._metadata[key]['matched'] = True

            # Update our metadata to accurately reflect the correct destination message
            self._metadata[key]['dest_offset'] = dest_message.offset()
            self._metadata[key]['dest_hash'] = dest_hash
            self._metadata[key]['dest_timestamp'] = dest_message.timestamp()[1]
            self._metadata[key]['dest_tpo'] = dest_tpo
            self._metadata[key]['dest_message'] = dest_message
            
            translated_offsets.append(dest_tpo)
            messages_found += 1

            # Found it so stop iterating
            break

    self.logger.info(f"Found {messages_found} out of {len(unmatched_offsets)} unmatched objects.")
    # Sort the offset map by partition number, which may have become out of
    # order if we needed to read and hash messages to find a hash match
    return sorted(translated_offsets, key=lambda k: k.partition) 


  def getMetadata(self):
   """Return our offset metadata object"""
   return self._metadata

  def getMessage(self, consumer, tpo):
    """Read a message at a tpo, return it"""
    consumer.assign([tpo])
    res = consumer.consume(num_messages=1, timeout=3)
    if len(res) == 1:
        return res[0]
    else:
        return None

  def commitTranslatedOffsets(self, tpos):
    """Given a list of TopicPartition objects, set the consumer group offsets"""

    self.logger.info("Committing offsets for supplied TPOs...")

    # Our offsets have been the last message consumed; need to set all offsets to +1
    # so that they represent the next message to consume.
    for t in tpos:
      t.offset += 1

    self.logger.info(" TPO offsets are incremented by one so that next message consumed is correct.")

    errored_commits = list()
    retries = 3
    while retries > 0:
      self.logger.info(f"Calling commit() for {len(tpos)} topic/partitions to destination cluster.")
      committed = self._dest_consumer.commit(offsets=tpos, asynchronous=False)

      for t in [t for t in committed if t.error is not None]:
        errored_commits.append(t)

      if len(errored_commits) > 0:
        self.logger.warning(f"Errors commiting offsets:")
        for t in errored_commits:
          self.logger.info(f"     Partition({t.partition}), Offset({t.offset}): {t.error}")
        self.logger.info(f"Trying again in 2 seconds...")
        time.sleep(2)
        tpos = errored_commits
        errored_commits = list()
        retries -= 1
      else:
        self.logger.info("Offsets committed successfully to destination cluster")
        errored_commits.clear()
        break

      if len(errored_commits) > 0:
        self.logger.warning("Still had errors after 3 tries:")
        for t in errored_commits:
          self.logger.info(f"     Partition({t.partition}), Offset({t.offset}): {t.error}")
        self.logger.info("Returning with a job not finished!!")

    return committed
    
        
    

  def printMetadata(self, metadata=None):
    if metadata is None:
      metadata = self._metadata

    #print("================================================================================")
    #print("================================================================================")
    #print("================================================================================")
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(metadata)
    #print("================================================================================")
    #print("================================================================================")
    #print("================================================================================")

    topic = None
    for key in metadata.keys():
      md = metadata[key]
      tpo =md['src_tpo']

      if tpo.topic != topic:
        topic = tpo.topic
        self.logger.info(f"topic: {tpo.topic}:")
        
      src_offset    = md['src_offset']
      src_timestamp = md['src_timestamp']
      src_hash      = md['src_hash']

      # We might be passed a metadata object that doesn't set dest_* fields
      if 'dest_tpo' in md:
        if md['dest_tpo'] is not None:
          dest_offset = md['dest_tpo'].offset
        else:
          dest_offset = ''
      else:
        dest_offset = ''

      if 'dest_message' in md:
        if md['dest_message'] is not None:
          dest_timestamp = md['dest_message'].timestamp()[1]
        else:
          dest_timestamp = ''
      else:
        dest_timestamp = ''

      
      if 'dest_hash' in md:
        dest_hash = md['dest_hash']
      else:
        dest_hash = ''
   
      self.logger.info(f"  p[{tpo.partition:1}]")
      self.logger.info(f"     source       last message offset ({src_offset:1}), timestamp({src_timestamp:12}), hash({src_hash})")       
      self.logger.info(f"     destination  last message offset ({dest_offset:1}), timestamp({dest_timestamp:12}), hash({dest_hash})")       

      #if 'src_message' in md and md['src_message'] is not None:
      #  pp.pprint(str(md['src_message'].value(),'utf-8'))
      #if 'dest_message' in md and md['dest_message'] is not None:
      #  pp.pprint(str(md['dest_message'].value(),'utf-8'))
      #print("<<<<<< DONE")

  def getConsumerGroupOffsets(self, topics):
    """Return the latest offset for the consumer group defined at
    object initialisation time.
    Moves offset by -1 so that we can re-read the last message consumed.
    """

    self.logger.info(f"Getting consumer group offsets for {len(topics)} topics...")

    tpos = self.getTPOs(topics)
    tpos = self._consumer.committed(tpos)

    self.logger.info("  Decrementing offsets so that we can inspect the last message consumed (for hashing, timestamps, etc)")
    # Wind back one offset so that we can re-read the messages
    for t in tpos:
        t.offset -= 1


    self.logger.info(f"Found offsets for {len(tpos)} topic partitions.")
    return tpos



  def findTopicsForConsumerGroup(self, cg=None):
    """Given a consumer group name, Find the topics associated with the consumer group.
    We use the shell because the confluent_kafka_python package doesn't yet provide this,
    see: https://github.com/confluentinc/confluent-kafka-python/issues/223
    """

    self.logger.info(f"Finding topics associated with {self._src_group_id}...")

    # Test that we have a kafka-consumer-groups handy...
    if subprocess.run(['which','kafka-consumer-groups']).returncode == 1:
        raise OSError("No 'kafka-consumer-groups' command found in $PATH")
    

    if cg is None:
        cg = self._src_group_id

    cmd = f"kafka-consumer-groups --bootstrap-server {self._src_bootstrap_servers} --describe --group {cg}  2>/dev/null| grep {cg} | awk '{{print $2}}' | sort -u"
    self.logger.info(f"Running {cmd}")
    res = subprocess.run(cmd,shell=True,stdout=subprocess.PIPE)

    cg_topics = list()
    for topic in str(res.stdout,'utf-8').split('\n'):
      if topic != '':
        cg_topics.append(topic) 

    # If we were configured to run for just one topic in a CG; then return just that topic,
    # but only if it exists in the CG
    if self._src_topic is not None and self._src_topic in cg_topics:
      self.logger.info("Overriding topic list from CG tool with supplied topic.")
      cg_topics = [self._src_topic]

    self.logger.info(f"Returning {cg_topics}...")
    return(cg_topics)
      

def setupLogging():
    """Log to console and file, with timestamps"""

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt="%(asctime)s [%(levelname)-5.5s]  %(message)s", datefmt="%Y-%m-%dT%H:%M:%S %Z")
    console.setFormatter(formatter)


    logger.addHandler(console)

    fileHandler = logging.FileHandler("{0}.log".format('offset_translator'))
    fileHandler.setFormatter(formatter)

    logger.addHandler(fileHandler)

    return logger




if __name__ == '__main__':
    logger = setupLogging()

    parser = argparse.ArgumentParser(description='Translate consumer offsets for a given consumer group')
    parser.add_argument('--source-broker', dest='src_broker', required=True, help='Source cluster to read consumer group offset info from')
    parser.add_argument('--dest-broker',   dest='dst_broker', required=True, help='Destination cluster to write translated consumer group offsets to')
    parser.add_argument('--group',         dest='group', required=True, default=None, help='A single consumer group to work with')
    parser.add_argument('--topic',         dest='topic', required=False, default=None, help='A single topic to work with, instead of an entire consumer group')

    args = parser.parse_args()

    if args.group is None and args.topic is None:
        print("Supply either a --group or a --topic argument")
        exit(1)

    
    logging.info("")
    logging.info("")
    logging.info("================================================================================")
    logging.info(f"Starting Offset translator.")
    logging.info(f"Args passed in are: {args}")

    ot = OffsetTranslator(
                src_bootstrap_server  = args.src_broker,
                src_group_id          = args.group,
                src_topic             = args.topic,
                dest_bootstrap_server = args.dst_broker,
                dest_group_id         = args.group)

    topics = ot.findTopicsForConsumerGroup()

    print(f"Getting committed offsets for {args.group} on topic {args.topic}") 
    tpos = ot.getConsumerGroupOffsets(topics)

    print(f"Building an initial (empty) metadata map of topic/partitions and their CG offsets")
    metadata = ot.buildMetadataMap(tpos)

    print("Printing metadata...")
    ot.printMetadata()

    print("Inspecting TPO messages...")
    metadata = ot.inspectTPOMessages(tpos)

    print("Updating metadata...")
    ot.updateMetadata(metadata)


    print("Getting destionatin cluster TPOs via source cluster message timestamps...")
    metadata = ot.getMetadata()
    dest_tpos = ot.getTPOsByTime(metadata=metadata)
     
    print("Inspecting destination cluster messages (hashing, etc)...")
    dest_metadata = ot.inspectTPOMessages(dest_tpos, cluster="destination")

    print("Updating metadata...")
    metadata = ot.updateMetadata(dest_metadata)

    print("Generating list of translated offsets...")
    translated_offsets = ot.compareOffsets()

    for o in translated_offsets:
        print(f"  topic: {o.topic}, partition {o.partition}, offset {o.offset}")
 
    res = ot.commitTranslatedOffsets(translated_offsets)
