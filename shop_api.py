import json
import os
import sys
import uuid

from kafka_client import create_producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("SHOP_PRODUCTS_TOPIC", "shop.products.raw")
PRODUCTS_FILE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("PRODUCTS_FILE", "data/products.json")

def load_products(path):
    with open(path, encoding="utf-8") as f:
        products = json.load(f)
    if not isinstance(products, list):
        raise ValueError("products file must contain a JSON array")
    return products


def main():
    products = load_products(PRODUCTS_FILE)
    producer = create_producer()

    for product in products:
        key = product.get("product_id") or str(uuid.uuid4())
        producer.send(TOPIC, key=key, value=product)
        print(f"sent: {key}")

    producer.flush()
    producer.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
