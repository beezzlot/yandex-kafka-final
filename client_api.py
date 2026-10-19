import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

from kafka_client import create_producer

SEARCH_TOPIC = os.getenv(
    "CLIENT_SEARCH_TOPIC",
    "client.search.requests",
)
RECOMMENDATION_TOPIC = os.getenv(
    "CLIENT_RECOMMENDATION_TOPIC",
    "client.recommendation.requests",
)
SEARCH_LIMIT = 20


def get_connection():
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
                CREATE TABLE IF NOT EXISTS client_requests (
                    request_id UUID PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    request_type TEXT NOT NULL,
                    query TEXT,
                    created_at TIMESTAMPTZ NOT NULL
                )
                """
            )


def build_request(request_type, client_id, query=None):
    return {
        "request_id": str(uuid.uuid4()),
        "client_id": client_id,
        "request_type": request_type,
        "query": query,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def save_request(conn, request):
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO client_requests (
                    request_id,
                    client_id,
                    request_type,
                    query,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    request["request_id"],
                    request["client_id"],
                    request["request_type"],
                    request["query"],
                    request["created_at"],
                ),
            )


def publish_request(producer, conn, topic, request):
    producer.send(
        topic,
        key=request["client_id"],
        value=request,
    ).get(timeout=30)

    save_request(conn, request)


def search_products(conn, query):
    escaped = (
        query.replace("!", "!!")
        .replace("%", "!%")
        .replace("_", "!_")
    )

    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT document
                FROM products
                WHERE document->>'name' ILIKE %s ESCAPE '!'
                ORDER BY document->>'name', store_id, product_id
                LIMIT %s
                """,
                (f"%{escaped}%", SEARCH_LIMIT + 1),
            )
            return [row[0] for row in cursor.fetchall()]


def print_products(products):
    if not products:
        print("Товары не найдены")
        return

    for number, product in enumerate(products[:SEARCH_LIMIT], start=1):
        price = product.get("price") or {}
        stock = product.get("stock") or {}

        print(
            f"\n{number}. {product.get('name', '')}\n"
            f"   ID: {product.get('product_id', '')}\n"
            f"   Магазин: {product.get('store_id', '')}\n"
            f"   Категория: {product.get('category', '')}\n"
            f"   Цена: {price.get('amount', '')} "
            f"{price.get('currency', '')}\n"
            f"   Доступно: {stock.get('available', 0)}\n"
            f"   Описание: {product.get('description', '')}"
        )

    if len(products) > SEARCH_LIMIT:
        print(f"\nПоказаны первые {SEARCH_LIMIT} товаров. Уточните запрос.")


def print_help():
    print(
        "Команды:\n"
        "  search <client_id> <название товара>\n"
        "  recommend <client_id>\n"
        "  help\n"
        "  exit"
    )


def handle_command(line, producer, conn):
    parts = line.split(maxsplit=2)
    command = parts[0].lower()

    if command == "help":
        print_help()
        return

    if command == "search":
        if len(parts) != 3:
            print("Использование: search <client_id> <название товара>")
            return

        client_id = parts[1]
        query = parts[2].strip()

        request = build_request("search", client_id, query)
        publish_request(producer, conn, SEARCH_TOPIC, request)

        print(f"search request sent: {request['request_id']}")
        print_products(search_products(conn, query))
        return

    if command == "recommend":
        if len(parts) != 2:
            print("Использование: recommend <client_id>")
            return

        request = build_request("recommendation", parts[1])
        publish_request(producer, conn, RECOMMENDATION_TOPIC, request)

        print(f"recommendation request sent: {request['request_id']}")
        print("Результат формируется асинхронно и доступен через ksqlDB")
        return

    print("Неизвестная команда. Введите help")


def main():
    conn = None
    producer = None

    try:
        conn = get_connection()
        init_db(conn)
        producer = create_producer()
        print_help()

        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not line:
                continue

            if line.lower() == "exit":
                break

            try:
                handle_command(line, producer, conn)
            except Exception as error:
                print(f"Ошибка выполнения команды: {error}", file=sys.stderr)
    finally:
        try:
            if producer is not None:
                producer.close(timeout=10)
        finally:
            if conn is not None:
                conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Ошибка запуска: {error}", file=sys.stderr)
        sys.exit(1)