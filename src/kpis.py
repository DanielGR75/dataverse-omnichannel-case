"""
Capa de KPIs — definiciones versionadas de métricas comerciales.

Cada función define UN KPI, documentado con su versión y criterio de
negocio explícito. Son reutilizables: se les pasa una lista de columnas
para agrupar (group_by), y sirven para cualquier corte -- país, categoría,
canal, mes, o combinaciones -- sin duplicar la lógica de negocio.

El resultado de aplicar estas funciones con los agrupamientos elegidos
para el executive view es lo que se materializa como capa 'gold'.
"""

import polars as pl

# Estados que cuentan como "venta real" (transacción efectivamente ocurrida)
REAL_SALE_STATUSES = ["delivered", "returned"]


def calculate_revenue_bruto(fact_orders: pl.DataFrame, group_by: list[str]) -> pl.DataFrame:
    """
    KPI v1.0 — Revenue Bruto

    Definición: suma de revenue_eur para líneas de producto con
    order_status en ('delivered', 'returned').

    Criterio de "venta real": se considera venta real toda orden que
    efectivamente generó una transacción -- salió de inventario, se
    facturó -- sin importar si después fue devuelta. Se excluye
    'processing' (resultado aún incierto) y 'cancelled' (nunca hubo
    transacción efectiva).

    Nota: revenue_eur nulo (wholesale con qty_sold=0) se trata como 0,
    consistente con que no hubo venta que registrar.
    """
    return (
        fact_orders
        .filter(pl.col("order_status").is_in(REAL_SALE_STATUSES))
        .group_by(group_by)
        .agg(pl.col("revenue_eur").fill_null(0).sum().alias("revenue_bruto_eur"))
    )


def calculate_return_rate(fact_orders: pl.DataFrame, group_by: list[str]) -> pl.DataFrame:
    """
    KPI v1.0 — Tasa de Devolución

    Definición: proporción del Revenue Bruto que corresponde a órdenes
    con order_status = 'returned'.

    Tasa de Devolución = SUM(revenue_eur WHERE returned)
                          / SUM(revenue_eur WHERE delivered OR returned)

    Sirve para identificar canales, países o categorías con problemas de
    calidad, talla, expectativa vs producto real, etc.
    """
    real_sales = fact_orders.filter(pl.col("order_status").is_in(REAL_SALE_STATUSES))

    agg = (
        real_sales
        .group_by(group_by + ["order_status"])
        .agg(pl.col("revenue_eur").fill_null(0).sum().alias("revenue_eur"))
        .pivot(values="revenue_eur", index=group_by, on="order_status")
    )

    for col in ["delivered", "returned"]:
        if col not in agg.columns:
            agg = agg.with_columns(pl.lit(0.0).alias(col))

    return agg.with_columns(
        pl.col("delivered").fill_null(0),
        pl.col("returned").fill_null(0),
    ).with_columns(
        (pl.col("returned") / (pl.col("delivered") + pl.col("returned")))
        .fill_nan(0)
        .alias("return_rate")
    ).select(group_by + ["return_rate"])


def calculate_fulfillment_rate(fact_orders: pl.DataFrame, group_by: list[str]) -> pl.DataFrame:
    """
    KPI v1.0 — Tasa de Cumplimiento (Fulfillment Rate)

    Definición: proporción de unidades pedidas que efectivamente se
    vendieron/entregaron.

    Fulfillment Rate = SUM(qty_sold) / SUM(qty_ordered)

    A diferencia de los otros 2 KPIs, aquí NO se filtra por order_status
    -- se usa qty_sold tal cual viene (ya es 0 en órdenes canceladas o
    en proceso), sobre el total de qty_ordered. Mide qué tan bien se
    satisface la demanda solicitada, útil para detectar problemas de
    stock o quiebre logístico por canal/país/categoría.
    """
    return (
        fact_orders
        .group_by(group_by)
        .agg([
            pl.col("qty_sold").sum().alias("_qty_sold_total"),
            pl.col("qty_ordered").sum().alias("_qty_ordered_total"),
        ])
        .with_columns(
            (pl.col("_qty_sold_total") / pl.col("_qty_ordered_total"))
            .alias("fulfillment_rate")
        )
        .select(group_by + ["fulfillment_rate"])
    )


def build_kpi_table(fact_orders: pl.DataFrame, group_by: list[str]) -> pl.DataFrame:
    """Combina los 3 KPIs en una sola tabla para un mismo corte dimensional."""
    revenue = calculate_revenue_bruto(fact_orders, group_by)
    returns = calculate_return_rate(fact_orders, group_by)
    fulfillment = calculate_fulfillment_rate(fact_orders, group_by)

    return (
        revenue
        .join(returns, on=group_by, how="left")
        .join(fulfillment, on=group_by, how="left")
        .sort(group_by)
    )