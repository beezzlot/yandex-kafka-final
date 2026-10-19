import json
import logging
import os
import ssl
from pathlib import Path

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logger = logging.getLogger(__name__)


def resolve_path(variable, default):
    path = Path(os.getenv(variable, default))
    return path if path.is_absolute() else BASE_DIR / path


def create_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "marketplace"),
        user=os.getenv("POSTGRES_USER", "marketplace"),
        password=os.getenv("POSTGRES_PASSWORD", "marketplace"),
        connect_timeout=10,
    )


def init_db(conn):
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    store_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    document JSONB NOT NULL,
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (store_id, product_id)
                )
                """
            )


def create_consumer():
    context = ssl.create_default_context(
        cafile=str(resolve_path("KAFKA_SSL_CAFILE", "ssl/ca.crt"))
    )
    context.load_cert_chain(
        certfile=str(resolve_path("KAFKA_SSL_CERTFILE", "ssl/app.crt")),
        keyfile=str(resolve_path("KAFKA_SSL_KEYFILE", "ssl/app.key")),
    )

    servers = [
        server.strip()
        for server in os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9094",
        ).split(",")
        if server.strip()
    ]

    return KafkaConsumer(
        os.getenv("SHOP_VALID_PRODUCTS_TOPIC", "shop.products.valid"),
        bootstrap_servers=servers,
        security_protocol="SSL",
        ssl_context=context,
        group_id=os.getenv("PRODUCTS_SINK_GROUP", "postgres-products-sink"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        max_poll_records=1,
    )


def save_product(conn, product):
    for field in ("store_id", "product_id"):
        if not isinstance(product.get(field), str) or not product[field].strip():
            raise ValueError(f"{field} должен быть непустой строкой")

    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO products (store_id, product_id, document)
                VALUES (%s, %s, %s)
                ON CONFLICT (store_id, product_id)
                DO UPDATE SET
                    document = EXCLUDED.document,
                    synced_at = NOW()
                """,
                (
                    product["store_id"],
                    product["product_id"],
                    Json(product),
                ),
            )


def main():
    conn = create_connection()
    consumer = None

    try:
        init_db(conn)
        consumer = create_consumer()
        logger.info("Ожидание товаров из shop.products.valid")

        for message in consumer:
            save_product(conn, message.value)

            consumer.commit(
                offsets={
                    message.topic_partition
                    if hasattr(message, "topic_partition")
                    else topic_partition(message): OffsetAndMetadata(
                        message.offset + 1,
                        "",
                    )
                }
            )

            logger.info(
                "saved: store=%s product=%s partition=%s offset=%s",
                message.value["store_id"],
                message.value["product_id"],
                message.partition,
                message.offset,
            )
    finally:
        if consumer is not None:
            consumer.close()
        conn.close()


def topic_partition(message):
    from kafka import TopicPartition

    return TopicPartition(message.topic, message.partition)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        main()
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")
    except Exception:
        logger.exception("Ошибка сохранения товаров")
        raise SystemExit(1)