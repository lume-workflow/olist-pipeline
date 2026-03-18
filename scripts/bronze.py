from pathlib import Path
import shutil
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RAW_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "product_category_name_translation.csv",
]


def bronze():
    base_dir = Path(__file__).resolve().parents[1]
    raw_dir = base_dir / "data" / "raw"
    bronze_dir = base_dir / "data" / "bronze"

    bronze_dir.mkdir(parents=True, exist_ok=True)

    for filename in RAW_FILES:
        source = raw_dir / filename
        destination = bronze_dir / filename

        if not source.exists():
            raise FileNotFoundError(f"Arquivo não encontrado em raw/: {filename}")

        shutil.copy2(source, destination)
        logger.info(f"Bronze: copiado {filename}")

    logger.info("Bronze concluída — 5 arquivos em data/bronze/")


if __name__ == "__main__":
    bronze()
