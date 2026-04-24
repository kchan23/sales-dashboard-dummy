#!/usr/bin/env python3
"""Query the Toast API and write a GUID → location name map to a text file.

Output: integrations/toast_api/restaurant_map.txt

Run from project root:
    python3 -m scripts.discover_restaurants
"""

from datetime import datetime
from pathlib import Path

from integrations.toast_api.client import ToastAPIClient

OUT_PATH = Path(__file__).parent.parent / "integrations" / "toast_api" / "restaurant_map.txt"


def main():
    client = ToastAPIClient()
    restaurants = client.discover_restaurants()

    lines = [
        "# Restaurant GUID -> location name",
        f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"# Source: Toast API /partners/v1/restaurants ({len(restaurants)} location(s))",
        "",
    ]

    for r in restaurants:
        guid = r.get("restaurantGuid") or r.get("guid", "UNKNOWN")
        name = r.get("restaurantName") or r.get("name", "")
        lines.append(f"{guid}\t{name}")

    OUT_PATH.write_text("\n".join(lines) + "\n")
    print(f"Wrote {len(restaurants)} location(s) to {OUT_PATH}")
    for line in lines[4:]:
        print(f"  {line}")


if __name__ == "__main__":
    main()
