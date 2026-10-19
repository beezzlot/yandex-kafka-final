import json
import os
import ssl

import faust

load_dotenv = None
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
CAFILE = os.getenv("KAFKA_SSL_CAFILE", "ssl/ca.crt")
CERTFILE = os.getenv("KAFKA_SSL_CERTFILE", "ssl/app.crt")
KEYFILE = os.getenv("KAFKA_SSL_KEYFILE", "ssl/app.key")
BANNED_FILE = os.getenv("BANNED_PRODUCTS_FILE", "data/banned_products.json")

SOURCE_TOPIC = "shop.products.raw"
VALID_TOPIC = "shop.products.valid"


def load_banned_ids(path):
    with open(path, encoding="utf-8") as f:
        items = json.load(f)
    return {item["product_id"] for item in items}


ssl_context = ssl.create_default_context(cafile=CAFILE)
ssl_context.load_cert_chain(CERTFILE, KEYFILE)

app = faust.App(
    "product-filter",
    broker=BOOTSTRAP,
    broker_credentials=ssl_context,
    value_serializer="raw",
)

products_topic = app.topic(SOURCE_TOPIC, value_type=bytes)
valid_products_topic = app.topic(VALID_TOPIC, value_type=bytes)

banned_ids = load_banned_ids(BANNED_FILE)


@app.agent(products_topic)
async def filter_products(products):
    async for product in products:
        data = json.loads(product)
        product_id = str(data.get("product_id"))

        if product_id in banned_ids:
            print(f"blocked: {product_id}")
            continue

        await valid_products_topic.send(key=product_id.encode(), value=product)
        print(f"allowed: {product_id}")

if __name__ == "__main__":
    app.main()