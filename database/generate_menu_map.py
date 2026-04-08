#!/usr/bin/env python3
"""Generate menu_canonical_map: maps each item_name to its Chinese canonical name, English display name, and category.

Items with Chinese characters use those characters as the canonical name (reliable
across both CSV and API data sources where English names may differ). The display_name
is the most-common English variant for that Chinese group. Items with no Chinese
characters keep their original name for all three columns.

Category is sourced from the inventory table (Toast menus API), which has the most
complete coverage. The most-common non-null category per item_name is used.

Run once after backfill (or whenever new menu items are added):
    python3 -m database.generate_menu_map
"""
import re
import logging
from google.cloud.bigquery import LoadJobConfig, SchemaField
from database.bigquery import BigQueryManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def extract_chinese(s: str) -> str:
    return ''.join(re.findall(r'[\u4e00-\u9fff]+', str(s)))


def extract_english(s: str) -> str:
    return re.sub(r'[\u4e00-\u9fff]+', '', str(s)).strip()


def clean_display_name(s: str) -> str:
    """Remove quantity suffixes, emojis, and special symbols from English display text."""
    # Remove (N) quantity/portion suffixes like (1), (8), (12)
    s = re.sub(r'\s*\(\d+\)\s*', ' ', s)
    # Remove emoji and miscellaneous symbol characters
    s = re.sub(
        r'[\U00010000-\U0010ffff'   # supplementary planes (emoji)
        r'\u2300-\u27ff'            # misc technical, dingbats, enclosed alphanumerics (e.g. Ⓥ)
        r'\u2b00-\u2bff'            # misc symbols and arrows
        r']', '', s
    )
    # Collapse multiple spaces and strip
    return re.sub(r' {2,}', ' ', s).strip()


def main():
    bq = BigQueryManager()

    logger.info("Querying item names with occurrence counts...")
    df = bq.client.query(
        f"SELECT item_name, COUNT(*) as cnt"
        f" FROM `{bq.dataset_ref}.order_items`"
        f" WHERE item_name IS NOT NULL AND item_name != ''"
        f" GROUP BY item_name"
    ).to_dataframe()

    df['chinese_key'] = df['item_name'].apply(extract_chinese)
    df['english_part'] = df['item_name'].apply(extract_english)

    # For items with Chinese: pick the most-frequent English variant per Chinese group
    chinese_df = df[df['chinese_key'] != ''].copy()
    display_map = (
        chinese_df.sort_values('cnt', ascending=False)
                  .groupby('chinese_key')
                  .apply(lambda g: clean_display_name(g.iloc[0]['english_part']))
    )

    def get_display(row):
        if not row['chinese_key']:
            return row['item_name']          # no Chinese → use original
        eng = display_map.get(row['chinese_key'], '').strip()
        return eng if eng else row['chinese_key']  # fallback: Chinese if no English part

    df['canonical_name'] = df['chinese_key'].where(df['chinese_key'] != '', df['item_name'])
    df['display_name'] = df.apply(get_display, axis=1)

    mapping = df[['item_name', 'canonical_name', 'display_name']].drop_duplicates('item_name')

    # Pull most-common non-null category per item from inventory (Toast menus API)
    logger.info("Querying categories from inventory table...")
    cat_df = bq.client.query(
        f"SELECT item_name, category, COUNT(*) as cnt"
        f" FROM `{bq.dataset_ref}.inventory`"
        f" WHERE NULLIF(category, '') IS NOT NULL"
        f" GROUP BY item_name, category"
        f" QUALIFY ROW_NUMBER() OVER (PARTITION BY item_name ORDER BY cnt DESC) = 1"
    ).to_dataframe()
    cat_df = cat_df[['item_name', 'category']].rename(columns={'category': 'inv_category'})
    mapping = mapping.merge(cat_df, on='item_name', how='left')
    mapping['category'] = mapping['inv_category']
    mapping = mapping.drop(columns=['inv_category'])

    with_chinese = (df['chinese_key'] != '').sum()
    with_category = mapping['category'].notna().sum()
    logger.info(f"Mapping covers {len(mapping)} distinct item names")
    logger.info(f"  With Chinese canonical: {with_chinese}")
    logger.info(f"  Kept as-is (no Chinese): {len(mapping) - with_chinese}")
    logger.info(f"  With category from inventory: {with_category} ({100*with_category/len(mapping):.0f}%)")

    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            SchemaField("item_name", "STRING"),
            SchemaField("canonical_name", "STRING"),
            SchemaField("display_name", "STRING"),
            SchemaField("category", "STRING"),
        ],
    )
    bq.client.load_table_from_dataframe(
        mapping, f"{bq.dataset_ref}.menu_canonical_map", job_config=job_config
    ).result()
    logger.info("menu_canonical_map written to BigQuery.")


if __name__ == "__main__":
    main()
