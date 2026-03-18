from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def silver():
    # --- Caminhos ---
    base_dir = Path(__file__).resolve().parents[1]
    bronze_dir = base_dir / "data" / "bronze"
    silver_dir = base_dir / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    # --- Leitura ---
    logger.info("Lendo arquivos da camada Bronze...")
    df_orders      = pd.read_csv(bronze_dir / "olist_orders_dataset.csv")
    df_items       = pd.read_csv(bronze_dir / "olist_order_items_dataset.csv")
    df_customers   = pd.read_csv(bronze_dir / "olist_customers_dataset.csv")
    df_products    = pd.read_csv(bronze_dir / "olist_products_dataset.csv")
    df_translation = pd.read_csv(bronze_dir / "product_category_name_translation.csv")

    # --- Limpeza ---
    logger.info("Limpando registros nulos...")
    df_orders.dropna(subset=["order_id", "customer_id"], inplace=True)
    df_items.dropna(subset=["order_id", "product_id", "price", "freight_value"], inplace=True)

    # --- Filtro ---
    logger.info("Filtrando pedidos com status 'delivered'...")
    df_orders = df_orders[df_orders["order_status"] == "delivered"]

    # --- Receita ---
    logger.info("Calculando receita por item...")
    df_items["revenue"] = df_items["price"] + df_items["freight_value"]

    # --- Joins ---
    logger.info("Fazendo joins entre as tabelas...")
    df = df_orders.merge(df_items, on="order_id", how="inner")
    df = df.merge(df_customers, on="customer_id", how="inner")
    df = df.merge(df_products, on="product_id", how="inner")
    df = df.merge(df_translation, on="product_category_name", how="left")

    # --- Salvando ---
    output_path = silver_dir / "olist_silver.csv"
    df.to_csv(output_path, index=False)

    logger.info(f"Silver concluída — {len(df)} registros em {output_path}")


if __name__ == "__main__":
    silver()
