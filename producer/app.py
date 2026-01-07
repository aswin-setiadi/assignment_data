from collections.abc import Iterator
from itertools import cycle
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Union

from kafka import KafkaProducer

from utils import handle_exit, set_partition

DEBUG= os.getenv("DEBUG")
SOURCE_PATH= os.getenv("SOURCE_PATH", "data/test")
CYCLE_DATA= bool(os.getenv("CYCLE_DATA"))
SLEEP= int(os.getenv("SLEEP", 0))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "email_threads"
logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)
set_partition(TOPIC, KAFKA_BROKER, logger)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    # value_serializer=lambda v: json.dumps(v.encode("utf-8"))
)
# producer.flush() # flush internal buffer, blocking process

def load_samples(samples_path:str, return_cycle:bool=True)-> Union[Iterator, List]:
    samples:List[Dict[str,Any]]=[]
    path= Path(samples_path)
    for file in path.iterdir():
        with open(file.absolute(), "r", encoding="latin-1") as f:
            content=f.read()
            doc_id=file.name.split(".")[0]
            samples.append({"doc_id":doc_id, "payload":json.dumps({"doc_id":doc_id, "text":content}).encode("utf-8")})
    if return_cycle:
        return cycle(samples)
    else:
        return samples

@handle_exit(logger, producer, flush_timeout=3)
def produce(samples:Union[Iterator[Dict[str,str]],List]):
    if isinstance(samples, List):
        for x in samples:
            producer.send(topic=TOPIC, value=x["payload"])
            logger.info(f"sent {x['doc_id']}")
            time.sleep(SLEEP)

    else:
        while True:
            content= next(samples)
            producer.send(topic=TOPIC, value=content["payload"])
            logger.info(f"sent {content['doc_id']}")
            time.sleep(SLEEP)


@handle_exit(logger, producer, flush_timeout=3)
def produce_debug():
    count=0
    while True:
        msg=f"hello{count}"
        payload={"doc_id":count, "text":msg}
        producer.send(topic=TOPIC, value=json.dumps(payload).encode("utf-8"))
        logger.info(f"sent {[payload]}")
        count+=1
        time.sleep(SLEEP)

@handle_exit(logger, producer, flush_timeout=3)
def produce_evaluation():
    paths=["producer/data/eval/6.txt",
           "producer/data/eval/6_1.txt",
           "producer/data/eval/6_1m.txt",
           "producer/data/eval/6_1_2.txt",
           "producer/data/eval/6_1_2m.txt",
           "producer/data/eval/6_1m_2.txt",
           "producer/data/eval/6_1m_2m.txt"]
    for p in paths:
        with open(p, "r", encoding="latin-1") as f:
            content=f.read()
        doc_id=p.split("/")[1].split(".")[0]
        payload=json.dumps({"doc_id":doc_id, "text":content}).encode("utf-8")
        producer.send(topic=TOPIC, value=payload)
        logger.info(f"sent {doc_id}")
        time.sleep(SLEEP)


if __name__ == "__main__":
    samples= load_samples(SOURCE_PATH, return_cycle=CYCLE_DATA)
    if DEBUG:
        # produce_debug()
        produce_evaluation()
    else:
        produce(samples)