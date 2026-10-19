import json
import os
import sys
import uuid
from datetime import datetime, timezone

import psycopg2

from kafka_client import create_producer

SEARCH_TOPIC = os.getenv("CLIENT_SEARCH_TOPIC", "client.search.requests")
RECOMMENDATION_TOPIC = os.getenv("CLIENT_RECOMMENDATION_TOPIC", "client.recommendation.requests")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "marketplace")
PG_USER = os.getenv("POSTGRES_USER", "marketplace")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketplace")


def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute(
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
    conn.commit()


def save_request(conn, request):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO client_requests (request_id, client_id, request_type, query, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                request["request_id"],
                request["client_id"],
                request["request_type"],
                request.get("query"),
                request["created_at"],
            ),
        )
    conn.commit()


def send_request(producer, topic, request):
    producer.send(topic, key=request["request_id"], value=request)
    producer.flush()


def build_request(request_type, client_id, query=None):
    return {
        "request_id": str(uuid.uuid4()),
        "client_id": client_id,
        "request_type": request_type,
        "query": query,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    conn = get_connection()
    init_db(conn)
    producer = create_producer()

    print("Commands:")
    print("  search <product name>")
    print("  recommend <client id>")
    print("  exit")

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not line:
            continue

        if line == "exit":
            break

        parts = line.split(maxsplit=1)
        command = parts[0].lower()

        if command == "search" and len(parts) == 2:
            request = build_request("search", "anonymous", parts[1])
            send_request(producer, SEARCH_TOPIC, request)
            save_request(conn, request)
            print(f"search request sent: {request['request_id']}")
        elif command == "recommend" and len(parts) == 2:
            request = build_request("recommendation", parts[1])
            send_request(producer, RECOMMENDATION_TOPIC, request)
            save_request(conn, request)
            print(f"recommendation request sent: {request['request_id']}")
        else:
            print("invalid command")

    producer.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)