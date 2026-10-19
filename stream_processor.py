import json
import logging
import os
import ssl
from pathlib import Path

import faust
from dotenv import load_dotenv

from banned_products import read_banned_products

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logger = logging.getLogger(__name__)

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9094",
)
SOURCE_TOPIC = os.getenv("SHOP_PRODUCTS_TOPIC", "shop.products.raw")
VALID_TOPIC = os.getenv("SHOP_VALID_PRODUCTS_TOPIC", "shop.products.valid")


def resolve_path(variable, default):
    path = Path(os.getenv(variable, default))
    return path if path.is_absolute() else BASE_DIR / path


ssl_context = ssl.create_default_context(
    cafile=str(resolve_path("KAFKA_SSL_CAFILE", "ssl/ca.crt"))
)
ssl_context.load_cert_chain(
    certfile=str(resolve_path("KAFKA_SSL_CERTFILE", "ssl/app.crt")),
    keyfile=str(resolve_path("KAFKA_SSL_KEYFILE", "ssl/app.key")),
)

app = faust.App(
    "product-filter",
    broker=BOOTSTRAP,
    broker_credentials=ssl_context,
    value_serializer="raw",
    topic_partitions=3,
    topic_replication_factor=3,
    web_port=6066,
)

products_topic = app.topic(SOURCE_TOPIC, value_type=bytes)
valid_products_topic = app.topic(VALID_TOPIC, value_type=bytes)


@app.agent(products_topic)
async def filter_products(products):
    async for product in products:
        data = json.loads(product)
        product_id = data["product_id"]

        if not isinstance(product_id, str) or not product_id.strip():
            raise ValueError("product_id должен быть непустой строкой")

        banned_products = read_banned_products()

        if product_id in banned_products:
            logger.info(
                "blocked: %s; reason: %s",
                product_id,
                banned_products[product_id],
            )
            continue

        await valid_products_topic.send(
            key=product_id.encode("utf-8"),
            value=product,
        )
        logger.info("allowed: %s", product_id)


if __name__ == "__main__":
    app.main()