"""Small administrative CLI used by initialization scripts."""

import argparse

from .config import get_settings
from .storage import DuckDBStore


def main() -> None:
    """Run one explicitly selected administrative command."""
    parser = argparse.ArgumentParser(prog="binance-momentum-lab")
    parser.add_argument("command", choices=["init-db"])
    args = parser.parse_args()
    if args.command == "init-db":
        settings = get_settings()
        settings.startup_safety_check()
        store = DuckDBStore(settings.database_path)
        try:
            store.initialize()
        finally:
            store.close()


if __name__ == "__main__":
    main()
