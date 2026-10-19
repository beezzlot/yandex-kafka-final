import json
import os
import ssl

import faust
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
CAFILE = os.getenv("KAFKA_SSL_CAFILE", "ssl/ca.crt")
CERTFILE = os.getenv("KAFKA_SSL_CERTFILE", "ssl/app.crt")
KEYFILE = os.getenv("KAFKA_SSL_KEYFILE", "ssl/app.key")

PRODUCTS_TOPIC = "shop.products.valid"
SEARCH_TOPIC = "client.search.requests"
RECOMMEND_TOPIC = "client.recommendation.requests"
RECOMMENDATIONS_TOPIC = "client.recommendations"

ssl_context = ssl.create_default_context(cafile=CAFILE)
ssl_context.load_cert_chain(CERTFILE, KEYFILE)

app = faust.App(
    "recommendation-service",
    broker=BOOTSTRAP,
    broker_credentials=ssl_context,
    value_serializer="raw",
    topic_partitions=3,
    topic_replication_factor=3,
    web_port=6067,
)

products_topic = app.topic(PRODUCTS_TOPIC, value_type=bytes)
search_topic = app.topic(SEARCH_TOPIC, value_type=bytes)
recommend_topic = app.topic(RECOMMEND_TOPIC, value_type=bytes)
recommendations_topic = app.topic(RECOMMENDATIONS_TOPIC, value_type=bytes)

product_catalog = app.Table("product_catalog", default=dict)
user_interests = app.Table("user_interests", default=list)


@app.agent(products_topic)
async def build_catalog(products):
    async for product in products:
        data = json.loads(product)
        product_catalog[str(data["product_id"])] = data
        print(f"catalog updated: {data['product_id']}")


@app.agent(search_topic)
async def handle_search(requests):
    async for request in requests:
        data = json.loads(request)
        client_id = data["client_id"]
        query = (data.get("query") or "").lower()

        matches = [
            product
            for product in product_catalog.values()
            if query in str(product.get("name", "")).lower()
        ]

        categories = {product.get("category") for product in matches if product.get("category")}
        interests = set(user_interests[client_id]) | categories
        user_interests[client_id] = sorted(interests)

        print(f"interests updated: {client_id} -> {user_interests[client_id]}")


@app.agent(recommend_topic)
async def handle_recommend(requests):
    async for request in requests:
        data = json.loads(request)
        client_id = data["client_id"]
        interests = set(user_interests[client_id])

        recommendations = [
            product
            for product in product_catalog.values()
            if product.get("category") in interests
        ][:5]

        payload = {
            "request_id": data["request_id"],
            "client_id": client_id,
            "recommendations": recommendations,
        }

        await recommendations_topic.send(
            key=client_id.encode(),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )

        print(f"recommendations sent: {client_id} -> {len(recommendations)}")


if __name__ == "__main__":
    app.main()