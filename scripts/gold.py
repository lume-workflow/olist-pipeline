from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def gold():
    # --- Caminhos ---
    base_dir = Path(__file__).resolve().parents[1]
    silver_dir = base_dir / "data" / "silver"
    gold_dir = base_dir / "data" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    # --- Leitura ---
    logger.info("Lendo arquivo da camada Silver...")
    df = pd.read_csv(silver_dir / "olist_silver.csv")

    # --- Tabela Fato ---
    # Uma linha por item de pedido com as métricas de negócio
    logger.info("Construindo fct_orders...")
    fct_orders = df[[
        "order_id",
        "customer_id",
        "product_id",
        "revenue",
        "price",
        "freight_value",
        "order_purchase_timestamp"
    ]].copy()

    # --- Dimensão Cliente ---
    # drop_duplicates garante uma linha por customer_id — sem repetição
    logger.info("Construindo dim_customer...")
    dim_customer = df[[
        "customer_id",
        "customer_city",
        "customer_state"
    ]].drop_duplicates(subset=["customer_id"]).copy()

    # --- Dimensão Produto ---
    logger.info("Construindo dim_product...")
    dim_product = df[[
        "product_id",
        "product_category_name_english"
    ]].drop_duplicates(subset=["product_id"]).copy()

    # --- Salvando ---
    fct_orders.to_csv(gold_dir / "fct_orders.csv", index=False)
    dim_customer.to_csv(gold_dir / "dim_customer.csv", index=False)
    dim_product.to_csv(gold_dir / "dim_product.csv", index=False)

    logger.info(f"Gold concluída — fct_orders: {len(fct_orders)} linhas | dim_customer: {len(dim_customer)} | dim_product: {len(dim_product)}")


if __name__ == "__main__":
    gold()
