"""
Conversión de moneda a EUR.

Diseño: no asume qué canales están en qué moneda -- lee el valor real de
la columna `currency` en cada fila. Si ya es 'EUR', no convierte. Si es
moneda local, hace join con currency_reference (por country + year_month)
y aplica EUR = local_amount / local_units_per_eur.
Esto evita hardcodear "ecomm y wholesale ya están en EUR": sigue
funcionando si mañana cambia qué canal reporta en qué moneda.
"""

from pathlib import Path
import polars as pl

SILVER_DIR = Path("data/silver")


def convert_to_eur(fact_orders: pl.DataFrame, currency_ref: pl.DataFrame) -> pl.DataFrame:
    rates = currency_ref.select(["country", "year_month", "local_units_per_eur"])

    df = fact_orders.join(rates, on=["country", "year_month"], how="left")

    # Validación: si currency != EUR y no hay tasa, algo falta en el mapeo
    missing_rate = df.filter(
        (pl.col("currency") != "EUR") & (pl.col("local_units_per_eur").is_null())
    )
    if missing_rate.height > 0:
        print(f"ADVERTENCIA: {missing_rate.height} filas sin tasa de cambio encontrada")
        print(missing_rate.select(["channel", "country", "year_month"]).unique())

    df = df.with_columns(
        pl.when(pl.col("currency") == "EUR")
        .then(pl.col("revenue_local"))
        .otherwise(pl.col("revenue_local") / pl.col("local_units_per_eur"))
        .alias("revenue_eur"),

        pl.when(pl.col("currency") == "EUR")
        .then(pl.col("unit_price"))
        .otherwise(pl.col("unit_price") / pl.col("local_units_per_eur"))
        .alias("unit_price_eur"),
    )

    df = df.drop("local_units_per_eur")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(SILVER_DIR / "fact_orders.parquet")

    return df