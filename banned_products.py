import json
import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BANNED_FILE = Path(
    os.getenv("BANNED_PRODUCTS_FILE", "data/banned_products.json")
)

if not BANNED_FILE.is_absolute():
    BANNED_FILE = BASE_DIR / BANNED_FILE


def read_banned_products():
    with BANNED_FILE.open(encoding="utf-8") as file:
        items = json.load(file)

    if not isinstance(items, list):
        raise ValueError("Список запретов должен быть JSON-массивом")

    products = {}

    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Каждая запись должна быть объектом")

        product_id = item.get("product_id")
        reason = item.get("reason", "")

        if not isinstance(product_id, str) or not product_id.strip():
            raise ValueError("product_id должен быть непустой строкой")

        if not isinstance(reason, str):
            raise ValueError("reason должен быть строкой")

        if product_id in products:
            raise ValueError(f"Повторяющийся product_id: {product_id}")

        products[product_id] = reason

    return products


def write_banned_products(products):
    BANNED_FILE.parent.mkdir(parents=True, exist_ok=True)

    items = [
        {"product_id": product_id, "reason": reason}
        for product_id, reason in sorted(products.items())
    ]

    temporary_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=BANNED_FILE.parent,
            prefix=".banned-products-",
            delete=False,
        ) as file:
            temporary_path = Path(file.name)
            json.dump(items, file, ensure_ascii=False, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_path, BANNED_FILE)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)