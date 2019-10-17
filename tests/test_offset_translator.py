import time
import pprint
import pytest
import logging
import datetime
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from offset_translator import OffsetTranslator

pytest_plugins = ['docker_compose']

pp = pprint.PrettyPrinter(indent=4)

# Some constants
TOPIC="offset_test_topic_01"
 
@pytest.fixture(scope="session")
def kafka_up(session_scoped_container_getter):
  """Wait for kafka to be available..."""

  # Assumes cp-all-in-one docker image, which has two ports, 29092 and 9092
  # We want 9092
  source = session_scoped_container_getter.get("broker1").network_info[0]
  dest   = session_scoped_container_getter.get("broker2").network_info[0]

  if source.hostname == '0.0.0.0':
    source.hostname='localhost'

  if dest.hostname == '0.0.0.0':
    dest.hostname='localhost'


  return (source, dest)

@pytest.fixture(scope="session")
def prep_kafka_data(kafka_up):
  logging.info("In prep-kafka data step")

  (source, dest) = kafka_up

  ac = AdminClient({"bootstrap.servers": f"{source.hostname}:{source.host_port}"})
  ac_dest = AdminClient({"bootstrap.servers": f"{dest.hostname}:{dest.host_port}"})

  p = Producer({"bootstrap.servers": f"{source.hostname}:{source.host_port}"})
  p_dest = Producer({"bootstrap.servers": f"{dest.hostname}:{dest.host_port}"})


  # Add other topics if needed...
  for test_topic in [TOPIC]:
    logging.info(f"Setting up topic {test_topic} for testing...")

    logging.info(f"Deleting old '{test_topic} topic, if any..")
    
    # In source cluster
    if test_topic in ac.list_topics().topics.keys():
      fut = ac.delete_topics([test_topic], operation_timeout=5, request_timeout=5)
      for topic, f in fut.items():
       try:
         f.result(timeout=5)
         logging.info(f"Source topic {topic} deleted.")
       except Exception as e:
         logging.info(f"Failed to delete source topic {topic}: {e}")

    # In destination cluster
    if test_topic in ac_dest.list_topics().topics.keys():
      fut = ac_dest.delete_topics([test_topic], operation_timeout=5, request_timeout=5)
      for topic, f in fut.items():
       try:
         f.result(timeout=5)
         logging.info(f"Destination topic {topic} deleted.")
       except Exception as e:
         logging.info(f"Failed to delete destination topic {topic}: {e}")

    # Need a dumb sleep here, see https://github.com/confluentinc/confluent-kafka-python/issues/524
    time.sleep(5)

    # Source cluster
    fut = ac.create_topics([NewTopic(topic=test_topic, num_partitions=3, replication_factor=1)], request_timeout=5)
    for topic, f in fut.items():
      try:
        f.result(timeout=5)
        logging.info(f"Topic {topic} created.")
      except Exception as e:
        logging.info(f"Failed to create topic {topic}: {e}")

    # Destination cluster
    fut = ac_dest.create_topics([NewTopic(topic=test_topic, num_partitions=3, replication_factor=1)], request_timeout=5)
    for topic, f in fut.items():
      try:
        f.result(timeout=5)
        logging.info(f"Topic {topic} created.")
      except Exception as e:
        logging.info(f"Failed to create topic {topic}: {e}")


    now = int(datetime.datetime.utcnow().timestamp() * 1000)

    ### Normal incrementing data
    # "Old" data on source cluster, not present on new cluster.. e.g. offsets are higher
    # on source cluster.
    p.produce(topic=test_topic, key="1", partition=0, timestamp=now - 100, value="val 0")
    p.produce(topic=test_topic, key="1", partition=0, timestamp=now - 100, value="val 0")
    p.produce(topic=test_topic, key="1", partition=0, timestamp=now - 100, value="val 0")
    p.produce(topic=test_topic, key="1", partition=0, timestamp=now - 100, value="val 0")
    p.produce(topic=test_topic, key="1", partition=0, timestamp=now - 100, value="val 0")
    p.produce(topic=test_topic, key="1", partition=0, timestamp=now + 0, value="val 1")
    p.produce(topic=test_topic, key="2", partition=0, timestamp=now + 1, value="val 2")
    p.produce(topic=test_topic, key="3", partition=0, timestamp=now + 2, value="val 3")

    # Same in destination cluster
    p_dest.produce(topic=test_topic, key="1", partition=0, timestamp=now + 0, value="val 1")
    p_dest.produce(topic=test_topic, key="2", partition=0, timestamp=now + 1, value="val 2")
    p_dest.produce(topic=test_topic, key="3", partition=0, timestamp=now + 2, value="val 3")

    ### Multiple messages per millisecond - high volume topic - with more data later
    p.produce(topic=test_topic, key="4", partition=1, timestamp=now + 220, value="val 4")
    p.produce(topic=test_topic, key="5", partition=1, timestamp=now + 220, value="val 5")
    p.produce(topic=test_topic, key="6", partition=1, timestamp=now + 220, value="val 6")
    p.produce(topic=test_topic, key="7", partition=1, timestamp=now + 221, value="val 7")

	 # Same in destination cluster
    p_dest.produce(topic=test_topic, key="4", partition=1, timestamp=now + 220, value="val 4")
    p_dest.produce(topic=test_topic, key="5", partition=1, timestamp=now + 220, value="val 5")
    p_dest.produce(topic=test_topic, key="6", partition=1, timestamp=now + 220, value="val 6")
    p_dest.produce(topic=test_topic, key="7", partition=1, timestamp=now + 221, value="val 7")

    ### Multiple messages per millisecond - high volume topic - at end of topic partiton
    p.produce(topic=test_topic, key="8", partition=2, timestamp=now + 230, value="val 8")
    p.produce(topic=test_topic, key="9", partition=2, timestamp=now + 230, value="val 9")
    p.produce(topic=test_topic, key="10", partition=2, timestamp=now + 230, value="val 10")

	 # Same in destination cluster
    p_dest.produce(topic=test_topic, key="8", partition=2, timestamp=now + 230, value="val 8")
    p_dest.produce(topic=test_topic, key="9", partition=2, timestamp=now + 230, value="val 9")
    p_dest.produce(topic=test_topic, key="10", partition=2, timestamp=now + 230, value="val 10")

    p.flush()
    p_dest.flush()

  

@pytest.fixture(scope="session")
def setup_translator(kafka_up):
  logging.info("Setting up translator...")

  (source, dest) = kafka_up

  #print(f"{source.hostname}:{source.host_port} and {dest.hostname}:{dest.host_port}")

  ot = OffsetTranslator(
		  src_bootstrap_server=f"{source.hostname}:{source.host_port}",
        src_group_id="offset_translator_test",
        src_topic = None,
        dest_bootstrap_server=f"{dest.hostname}:{dest.host_port}",
        dest_group_id="offset_translator_test")
  
  
  return ot


@pytest.mark.prepdata
def test_01(prep_kafka_data):
   logging.info("Set up testing data on the cluster")

   # Prep the data producer
   prep_kafka_data  


def test_02(setup_translator):
   logging.info("Set consumer group offsets and confirm they are set properly")

   ot = setup_translator
   c = ot._consumer

   # Offset of the next message to be read by the consumer
   tpos = [TopicPartition(topic=TOPIC, partition=0, offset=6),
           TopicPartition(topic=TOPIC, partition=1, offset=2),
           TopicPartition(topic=TOPIC, partition=2, offset=1)]

   logging.info(f"Committing offsets synchronously for supplied tpos...")
   tpos_committed = c.commit(offsets=tpos, asynchronous=False)
   for tpo in tpos_committed:
     assert tpo.error is None, f"There is an issue with the topic {tpo.topic} /partition {tpo.partition} combination: {tpo.error}"

   assert tpos_committed == c.committed(tpos), "Committed offsets do not match previously specified manual offsets!"
  

	# Set the destination cluster to offsets 0
   c = ot._dest_consumer

   # Offset of the next message to be read by the consumer
   tpos = [TopicPartition(topic=TOPIC, partition=0, offset=0),
           TopicPartition(topic=TOPIC, partition=1, offset=0),
           TopicPartition(topic=TOPIC, partition=2, offset=0)]

   logging.info(f"Committing offsets synchronously for supplied tpos...")
   tpos_committed = c.commit(offsets=tpos, asynchronous=False)
   for tpo in tpos_committed:
     assert tpo.error is None, f"There is an issue with the topic {tpo.topic} /partition {tpo.partition} combination: {tpo.error}"

   assert tpos_committed == c.committed(tpos), "Committed offsets do not match previously specified manual offsets!"


def test_03(setup_translator):
  logging.info("We should have the topic TOPIC in our consumer group")

  ot = setup_translator
  topics = ot.findTopicsForConsumerGroup()
  assert topics == [TOPIC]


def test_04(setup_translator):
  logging.info("confirm that offsets are wound back by -1")

  ot = setup_translator
  topics = ot.findTopicsForConsumerGroup()
  tpos = sorted(ot.getConsumerGroupOffsets(topics), key=lambda tpo: tpo.partition) 
  
  assert tpos[0].partition == 0 and tpos[0].offset == 5 and \
         tpos[1].partition == 1 and tpos[1].offset == 1 and \
         tpos[2].partition == 2 and tpos[2].offset == 0,    \
        "consumer group offsets not set correctly for testing"



def test_05(setup_translator):
  logging.info("Confirm building metadata map includes TPOs as requested.")

  ot = setup_translator
  tpos = [TopicPartition(TOPIC,0,5),TopicPartition(TOPIC,1,0),TopicPartition(TOPIC,2,0)]
  # Build metadata map
  metadata = ot.buildMetadataMap(tpos)

  assert len(metadata) == 3, f"Expected three entries in metadata hash but only have {len(metadata)}"


def test_06(setup_translator):
  logging.info("Confirm that inspectTPOMessages does in fact return the correct message offset")

  ot = setup_translator
  tpos = [TopicPartition(TOPIC,0,0),TopicPartition(TOPIC,1,1),TopicPartition(TOPIC,2,2)]

  src_metadata = ot.inspectTPOMessages(tpos)

  assert src_metadata[f"{TOPIC}::0"]['src_offset'] == 0, "Offset for topic/1 is not 0!"
  assert src_metadata[f"{TOPIC}::1"]['src_offset'] == 1, "Offset for topic/1 is not 1!"
  assert src_metadata[f"{TOPIC}::2"]['src_offset'] == 2, "Offset for topic/1 is not 2!"
	

def test_07(setup_translator):
  logging.info("Update metadata after completing source cluster message inspection")

  ot = setup_translator
  tpos = [TopicPartition(TOPIC,0,5),TopicPartition(TOPIC,1,1),TopicPartition(TOPIC,2,0)]

  src_metadata = ot.inspectTPOMessages(tpos)
  ot.updateMetadata(src_metadata)
  metadata = ot.getMetadata()

  assert metadata[f"{TOPIC}::0"]['src_hash'] is not None
  assert metadata[f"{TOPIC}::1"]['src_hash'] is not None
  assert metadata[f"{TOPIC}::2"]['src_hash'] is not None


def test_08(setup_translator):
  logging.info("Confirm offsets in destination cluster are 0 for times provided from source cluster.")

  ot = setup_translator
  tpos = [TopicPartition(TOPIC,0,0),TopicPartition(TOPIC,1,1),TopicPartition(TOPIC,2,2)]

  src_metadata = ot.inspectTPOMessages(tpos)
  ot.updateMetadata(src_metadata)
  metadata = ot.getMetadata()

  dest_tpos = ot.getTPOsByTime(metadata=metadata)

  for t in dest_tpos:
    assert t.offset == 0, "offset for TPO is not 0"

  return (metadata, dest_tpos)

def test_09(setup_translator):
  logging.info("metadata is updated with destination cluster offsets, tpos, hashes and messages")

  ot = setup_translator
  tpos = [TopicPartition(TOPIC,0,5),TopicPartition(TOPIC,1,1),TopicPartition(TOPIC,2,0)]

  src_metadata = ot.inspectTPOMessages(tpos)

  ot.updateMetadata(src_metadata)
  metadata = ot.getMetadata()

  dest_tpos = ot.getTPOsByTime(metadata=metadata)

  
  dest_metadata = ot.inspectTPOMessages(dest_tpos, cluster="destination")
  metadata = ot.updateMetadata(dest_metadata)

  ot.printMetadata()

  # partitions 0 and 2 will match
  for i in range(0,3,2):
	  md = metadata[f"{TOPIC}::{i}"]
	  assert md['src_hash'] == md['dest_hash'], "Source and destination hashes should match, but don't"
  
  # partition 1 won't match because we had multiple messages for the same millisecond
  md = metadata[f"{TOPIC}::1"]
  assert md['src_hash'] != md['dest_hash'], "Source and destination hashes should differ, but are the same."
  

  
def test_10(setup_translator):
  logging.info("proper offsets are recorded in destination cluster")

  ot = setup_translator
  tpos = [TopicPartition(TOPIC,0,5),TopicPartition(TOPIC,1,1),TopicPartition(TOPIC,2,0)]

  src_metadata = ot.inspectTPOMessages(tpos)
  ot.updateMetadata(src_metadata)
  metadata = ot.getMetadata()

  dest_tpos = ot.getTPOsByTime(metadata=metadata)
  
  dest_metadata = ot.inspectTPOMessages(dest_tpos, cluster="destination")
  metadata = ot.updateMetadata(dest_metadata)
  offsets = ot.compareOffsets()
  
  pp.pprint(offsets)

def test_20(setup_translator):
  logging.info("Confirm that offsets are incremented by +1 before committing")

  ot = setup_translator

  tpos = [TopicPartition(TOPIC,0,0),TopicPartition(TOPIC,0,0),TopicPartition(TOPIC,0,0)]
  res = ot.commitTranslatedOffsets(tpos)

  counter = 0
  for t in res:
    counter += t.offset

  assert counter == 3, "Offsets not incremented by +1 before comitting"
