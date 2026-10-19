import argparse
import fcntl
import sys

from banned_products import (
    BANNED_FILE,
    read_banned_products,
    write_banned_products,
)


def main():
    parser = argparse.ArgumentParser(
        description="Управление списком запрещённых товаров"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("list")

    add_parser = commands.add_parser("add")
    add_parser.add_argument("product_id")
    add_parser.add_argument("--reason", default="Запрещён к продаже")

    remove_parser = commands.add_parser("remove")
    remove_parser.add_argument("product_id")

    args = parser.parse_args()

    if args.command != "list" and not args.product_id.strip():
        parser.error("product_id не должен быть пустым")

    BANNED_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_path = BANNED_FILE.with_suffix(BANNED_FILE.suffix + ".lock")

    with lock_path.open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)

        try:
            products = read_banned_products()
        except FileNotFoundError:
            products = {}

        if args.command == "list":
            if not products:
                print("Список запрещённых товаров пуст")
                return

            for product_id, reason in sorted(products.items()):
                print(f"{product_id}\t{reason}")
            return

        if args.command == "add":
            products[args.product_id] = args.reason
            write_banned_products(products)
            print(f"Запрещён: {args.product_id}")
            return

        if args.product_id not in products:
            print(f"Товар отсутствует в списке: {args.product_id}")
            return

        del products[args.product_id]
        write_banned_products(products)
        print(f"Запрет снят: {args.product_id}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"Ошибка: {error}", file=sys.stderr)
        sys.exit(1)