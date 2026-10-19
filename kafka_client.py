import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL")
CAFILE = os.getenv("KAFKA_SSL_CAFILE")
CERTFILE = os.getenv("KAFKA_SSL_CERTFILE")
KEYFILE = os.getenv("KAFKA_SSL_KEYFILE")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        security_protocol=SECURITY_PROTOCOL,
        ssl_cafile=CAFILE,
        ssl_certfile=CERTFILE,
        ssl_keyfile=KEYFILE,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=5,
    )