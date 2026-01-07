from functools import wraps
from logging import Logger

from kafka import KafkaConsumer, KafkaProducer

def handle_exit(logger:Logger, kp:KafkaProducer, flush_timeout:int=3):
    """Decorator factory that takes an argument."""
    def decorator(func):
        @wraps(func)  # Preserves the original function's metadata
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except KeyboardInterrupt:
                pass
            except Exception as e:
                logger.exception(e)
            finally:
                kp.flush(flush_timeout)
        return wrapper
    return decorator

def set_partition(topic:str, kafka_broker_uri:str, logger:Logger):
    logger.info(f"checking partition count for {topic=}")
    consumer= KafkaConsumer(bootstrap_servers=kafka_broker_uri)
    partitions= consumer.partitions_for_topic(topic)
    logger.info(f"{topic=} have {len(partitions)} partitions, closing consumer...")
    consumer.close()
    logger.info(f"consumer closed...")
