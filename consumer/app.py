import json
import logging
import os
import traceback

from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient

from email_thread_processing import ThreadProcessing
logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

# TODO change back localhost to kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://mongo:27017")
CREATE_INDEX= os.getenv("CREATE_INDEX")
TOPIC = "email_threads"
DLQ_TOPIC= f"dlq-{TOPIC}"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="email-consumers",
    auto_offset_reset="earliest",
    enable_auto_commit=True
    # value_deserializer= lambda v: json.loads(v)
)
# consumer.poll()
dlq_producer= KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
mongo_client = MongoClient(MONGO_URI)
if CREATE_INDEX:
    mongo_client[ThreadProcessing.DB_NAME][ThreadProcessing.CT_COLL_NAME].create_index([("Subject",1),("rank",1)],unique=True)
    mongo_client[ThreadProcessing.DB_NAME][ThreadProcessing.CT_COLL_NAME].create_index([("doc_ids",1)])
    mongo_client[ThreadProcessing.DB_NAME][ThreadProcessing.RE_COLL_NAME].create_index([("doc_id",1)], unique=True)

def consume(consumer_close_timeout:int=3, dlq_producer_flush_timeout:int=3):
    try:
        for msg in consumer:
            #TODO check msg properties: key headers, partition, offset, timestamp
            payload= json.loads(msg.value)
            emails= ThreadProcessing.split_thread(payload["text"])
            ThreadProcessing.create_canonical_thread(payload["doc_id"], payload["text"], emails, mongo_client, max_try=2)
            logger.info(f"Processed doc_id:{payload['doc_id']}")
            #alternative to enable_auto_commit, flag this offset as processed so next time restarted, will start from this
            # consumer.commit()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(e)
        headers=[("error",str(e).encode("utf-8")), ("traceback",traceback.format_exc().encode("utf-8"))]
        dlq_producer.send(topic=DLQ_TOPIC, value= msg.value, headers=headers)
        # consumer.commit()
    finally:

        consumer.close(timeout_ms=consumer_close_timeout*1000)
        dlq_producer.flush(dlq_producer_flush_timeout)
        logger.info("consumer exit...")

def flush_messages():
    for msg in consumer:
        try:
            payload= json.loads(msg.value)
            logger.info(f"flushing doc_id={payload['doc_id']}")
        finally:
            consumer.close()
            dlq_producer.flush()
            logger.info("consumer exit...")

def test_mongodb():
    logger.info(f"testing mongodb connection...")
    mongo_client["testdb"]["testcollection"].insert_one({"hello":"world"})

if __name__=="__main__":
    #consumer.poll() # manual trigger to join group properly
    consume()
    # flush_messages()
    # test_mongodb()