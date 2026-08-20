"""
Capa Silver — transformación a esquema común (fact_orders).

Diseño: separamos QUÉ cambia entre fuentes (configuración: mapeo de
nombres, diccionarios de normalización) de CÓMO se aplica (una función
genérica de transformación). Así, un cambio de nombre de columna en una
fuente se resuelve editando la configuración, no la lógica.

Única excepción: ecomm requiere un paso previo de "aplanado" (explode +
unnest) porque llega con lineas anidadas -- una fuente puede necesitar
pre-procesamiento propio antes de entrar al mapeo común, y es más claro
manejarlo explícito que forzarlo dentro de la función genérica.
"""

from pathlib import Path
import polars as pl

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")

# Esquema objetivo -- el mismo para las 5 fuentes
SILVER_COLUMNS = [
    "channel", "consumer_id", "order_id", "country", "year_month",
    "category", "product_id", "qty_ordered", "qty_sold", "unit_price",
    "currency", "revenue_local", "order_status", "adiclub",
]

COUNTRY_NAME_TO_ISO2 = {
    "Brazil": "BR", "Mexico": "MX", "Colombia": "CO",
    "Chile": "CL", "Argentina": "AR", "Peru": "PE",
}
COUNTRY_ISO3_TO_ISO2 = {
    "BRA": "BR", "MEX": "MX", "COL": "CO",
    "PER": "PE", "ARG": "AR", "CHL": "CL",
}
CATEGORY_FIX = {
    "Runing": "Running", "Outdor": "Outdoor",
    "Footbal": "Football", "Originls": "Originals",
    "Running": "Running", "Outdoor": "Outdoor",
    "Football": "Football", "Originals": "Originals",
}
ECOMM_CATCODE_TO_CATEGORY = {1: "Running", 2: "Outdoor", 3: "Football", 4: "Originals"}
WHOLESALE_CATCODE_TO_CATEGORY = {"RUN": "Running", "OUT": "Outdoor", "FBL": "Football", "ORG": "Originals"}
SPANISH_MONTH_TO_NUM = {
    "Ene": "01", "Feb": "02", "Mar": "03", "Abr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Ago": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dic": "12",
}
FRANCHISE_STATUS_MAP = {
    "En proceso": "processing", "Entregado": "delivered",
    "Cancelado": "cancelled", "Devuelto": "returned",
}


# ---------- Configuración por canal ----------
# Cada entrada define: rename (bronze -> nombre común) y una lista de
# pasos de normalización a aplicar después del rename.

def _config_retail():
    return {
        "channel": "retail",
        "rename": {
            "consumer_id": "consumer_id", "store_country": "country",
            "order_month": "year_month", "product_category": "category",
            "sku": "product_id", "units_ordered": "qty_ordered",
            "units_sold": "qty_sold", "unit_price": "unit_price",
            "currency": "currency", "transaction_id": "order_id",
            "loyalty_flag": "adiclub", "order_state": "order_status",
        },
    }

def _config_ecomm():
    return {
        "channel": "ecomm",
        "rename": {
            "customerId": "consumer_id", "market": "country",
            "period": "year_month", "orderId": "order_id",
            "currency": "currency", "adiClub": "adiclub", "status": "order_status",
            "catCode": "category", "article": "product_id",
            "qtyOrd": "qty_ordered", "qtySold": "qty_sold", "priceFinal": "unit_price",
        },
    }

def _config_app():
    return {
        "channel": "app",
        "rename": {
            "uid": "consumer_id", "cc": "country", "yearmonth": "year_month",
            "category": "category", "productId": "product_id", "qty": "qty_ordered",
            "sold": "qty_sold", "amount": "revenue_local", "currency": "currency",
            "orderRef": "order_id", "member": "adiclub", "fulfillment": "order_status",
        },
    }

def _config_wholesale():
    return {
        "channel": "wholesale",
        "rename": {
            "country_code": "country", "category_code": "category",
            "yr_month": "year_month",
            "material_id": "product_id", "sell_in_units": "qty_ordered",
            "sell_out_units": "qty_sold", "avg_price": "unit_price",
            "currency": "currency", "status": "order_status",
        },
    }

def _config_franchise():
    return {
        "channel": "franchise",
        "rename": {
            "Consumidor": "consumer_id", "Pais": "country", "Mes": "year_month",
            "Categoria": "category", "Codigo_Producto": "product_id",
            "Cant_Pedida": "qty_ordered", "Cant_Vendida": "qty_sold",
            "Precio_Unitario": "unit_price", "Moneda": "currency",
            "Num_Orden": "order_id", "AdiClub": "adiclub", "Estado_Orden": "order_status",
        },
    }


# ---------- Pre-paso especial: solo ecomm necesita aplanarse ----------

def _flatten_ecomm(df: pl.DataFrame) -> pl.DataFrame:
    """Ecomm llega con 'lines' como lista anidada (una fila = una orden
    con varios productos). La explotamos a una fila por línea de producto,
    igual grano que las demás fuentes. Esta es la única línea de código
    que difiere estructuralmente entre fuentes."""
    return df.explode("lines").unnest("lines")


# ---------- Normalización por campo (aplican donde el campo exista) ----------

def _normalize(df: pl.DataFrame, channel: str) -> pl.DataFrame:
    exprs = []

    if channel == "retail":
        exprs.append(pl.col("country").replace(COUNTRY_NAME_TO_ISO2))
    if channel == "app":
        exprs.append(pl.col("country").replace(COUNTRY_ISO3_TO_ISO2))

    if channel in ("app", "retail", "franchise"):
        exprs.append(pl.col("category").replace(CATEGORY_FIX))
    if channel == "ecomm":
        exprs.append(
            pl.col("category")
            .cast(pl.Utf8)
            .replace(
                {str(code): category for code, category in ECOMM_CATCODE_TO_CATEGORY.items()},
                return_dtype=pl.Utf8,
            )
        )   
    if channel == "wholesale":
        exprs.append(
            pl.col("category").replace(WHOLESALE_CATCODE_TO_CATEGORY, return_dtype=pl.Utf8)
        )

    if channel == "retail":
        exprs.append(
            pl.when(pl.col("adiclub") == "Y").then(True)
            .when(pl.col("adiclub") == "N").then(False)
            .otherwise(None)
            .alias("adiclub")
        )
    if channel == "franchise":
        exprs.append(
            pl.when(pl.col("adiclub") == "Si").then(True)
            .when(pl.col("adiclub") == "No").then(False)
            .otherwise(None)
            .alias("adiclub")
        )
    if channel == "app":
        exprs.append(
            pl.when(pl.col("adiclub") == "Y").then(True)
            .otherwise(None)
            .alias("adiclub")
        )
    # ecomm: adiClub ya viene como Boolean nativo desde el bronze, no necesita transformación

    if channel == "franchise":
        exprs.append(pl.col("order_status").replace(FRANCHISE_STATUS_MAP))
    else:
        exprs.append(pl.col("order_status").str.to_lowercase())

    if channel == "franchise":
        # "Abr-2025" -> "2025-04"
        exprs.append(
            pl.col("year_month").str.split("-").list.get(0).replace(SPANISH_MONTH_TO_NUM).alias("_mm"),
        )
    elif channel == "ecomm":
        # 202503 (int) -> "2025-03"
        exprs.append(pl.col("year_month").cast(pl.Utf8))
    elif channel == "app":
        # "2025/03" -> "2025-03"
        exprs.append(pl.col("year_month").str.replace("/", "-"))
    elif channel == "wholesale":
        # Datetime -> "2025-03"
        exprs.append(pl.col("year_month").dt.strftime("%Y-%m"))

    return df.with_columns(exprs) if exprs else df


def _finalize_year_month(df: pl.DataFrame, channel: str) -> pl.DataFrame:
    """Segundo paso para year_month: casos que necesitan una columna auxiliar
    (franchise, ecomm) se resuelven aquí, después de tener country/category listos."""
    if channel == "franchise":
        df = df.with_columns(
            (pl.col("year_month").str.split("-").list.get(1) + "-" + pl.col("_mm")).alias("year_month")
        ).drop("_mm")
    elif channel == "ecomm":
        df = df.with_columns(
            pl.col("year_month").str.slice(0, 4) + "-" + pl.col("year_month").str.slice(4, 2)
        )
    return df


def transform_to_silver(bronze_df: pl.DataFrame, config: dict) -> pl.DataFrame:
    channel = config["channel"]

    df = bronze_df.rename(config["rename"])
    df = _normalize(df, channel)
    df = _finalize_year_month(df, channel)

    # revenue_local: ya viene calculado en 'app' (via amount);
    # para las demás, se deriva de qty_sold * unit_price
    if "revenue_local" not in df.columns:
        df = df.with_columns(
            (pl.col("qty_sold") * pl.col("unit_price")).alias("revenue_local")
        )

    # unit_price: 'app' no la trae directo, la derivamos desde revenue_local
    # (protegido contra división por cero cuando qty_sold = 0)
    if "unit_price" not in df.columns:
        df = df.with_columns(
            pl.when(pl.col("qty_sold") > 0)
            .then(pl.col("revenue_local") / pl.col("qty_sold"))
            .otherwise(None)
            .alias("unit_price")
        )

    df = df.with_columns([
        pl.lit(channel).alias("channel"),
        pl.col("consumer_id").cast(pl.Utf8) if "consumer_id" in df.columns else pl.lit(None).alias("consumer_id"),
        pl.col("order_id").cast(pl.Utf8) if "order_id" in df.columns else pl.lit(None).alias("order_id"),
        pl.col("product_id").cast(pl.Utf8),
    ])

    for col in ["consumer_id", "order_id"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    if "adiclub" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Boolean).alias("adiclub"))

    return df.select(SILVER_COLUMNS)


CHANNEL_CONFIGS = {
    "retail": _config_retail,
    "ecomm": _config_ecomm,
    "app": _config_app,
    "wholesale": _config_wholesale,
    "franchise": _config_franchise,
}


def build_silver_layer(bronze_data: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Recorre las 5 fuentes de negocio (excluye currency_reference),
    aplica el flatten especial a ecomm, transforma cada una con su config,
    y concatena todo en una sola tabla fact_orders."""
    results = []
    for bronze_name, df in bronze_data.items():
        channel = bronze_name.replace("_orders", "")
        if channel not in CHANNEL_CONFIGS:
            continue  # ej. currency_reference, se maneja aparte

        if channel == "ecomm":
            df = _flatten_ecomm(df)

        config = CHANNEL_CONFIGS[channel]()
        silver_df = transform_to_silver(df, config)
        results.append(silver_df)

    fact_orders = pl.concat(results, how="vertical")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    fact_orders.write_parquet(SILVER_DIR / "fact_orders.parquet")

    return fact_orders