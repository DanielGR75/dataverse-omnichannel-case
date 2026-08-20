# DataVerse — Modelo Omnichannel Gobernado (Manager Omnichannel Analytics)

> **Estado:** provisional — pendiente incorporar capturas del dashboard Power BI
> y ajustar cifras finales antes de la entrega.

## 1. Objetivo del proyecto

Adidas vende a través de 5 canales (Retail, eCommerce/dotCom, App, Franchise,
Wholesale) en 6 países de LATAM (AR, BR, CL, CO, MX, PE). Cada canal reporta
en un sistema, formato y definición distintos, sin una vista unificada de
qué se vendió, cómo se vendió, y cómo se comporta el producto entre
categorías y países.

Este proyecto construye un **modelo de datos gobernado** que unifica los 5
canales en un esquema común, define métricas comerciales estándar con
criterios explícitos de "venta real", y traduce ese modelo en una vista
ejecutiva con un insight accionable y un plan de gobernanza/escalabilidad.

## 2. Arquitectura

Se usó arquitectura medallion (bronze → silver → gold), pensada para portar
directo a un lakehouse tipo Databricks/Delta Lake si el equipo lo requiere:

```
data/raw/      → 5 fuentes originales + currency_reference.csv, sin tocar
data/bronze/   → ingesta cruda estandarizada a Parquet, con metadata de
                 trazabilidad (_source, _ingested_at). Sin transformar
                 nombres ni valores.
data/silver/   → fact_orders.parquet — esquema común, 5 canales unificados,
                 moneda convertida a EUR.
data/gold/     → tablas de KPIs agregadas, listas para consumo ejecutivo.
```

```
src/
├── bronze.py    → ingesta genérica y auto-descubrimiento de fuentes
├── silver.py     → mapeo y normalización por canal (config + función genérica)
├── currency.py   → conversión a EUR
└── kpis.py       → definiciones de KPI, versionadas y reutilizables
```

```
notebooks/
├── 00_exploration.ipynb        → EDA inicial, descubrimiento de reglas de negocio
├── 01_bronze_ingestion.ipynb   → orquesta la ingesta bronze
├── 02_silver_transformation.ipynb → orquesta transformación + conversión EUR
└── 03_gold_kpis.ipynb          → genera las tablas gold
```

## 3. Decisiones de stack (y por qué)

| Decisión | Elegido | Por qué |
|---|---|---|
| Motor de datos | **Polars** (+ soporte puntual de lógica SQL-like) | Rendimiento muy superior a pandas para ~1.2M filas con múltiples joins y agregaciones repetidas durante la iteración; tipado más estricto que ayuda a exponer inconsistencias de origen en vez de esconderlas. |
| Plataforma cloud | **Ninguna (local)**, NO Databricks Free Edition | Free Edition impone cuotas de compute que se agotan con iteración normal y bloquean el entorno sin aviso — riesgo inaceptable con deadline fijo. El pipeline está diseñado en capas bronze/silver/gold (arquitectura medallion) para portarse 1:1 a Databricks/Delta Lake si el equipo lo requiere. |
| Vista ejecutiva | **Power BI** | Es la herramienta que ya domina el equipo de Adidas — prioriza adopción organizacional sobre novedad. Las tablas gold están en Parquet, listas para conectar directo. |
| Entorno de desarrollo | VS Code + Jupyter + venv | Reproducible en cualquier máquina sin dependencias de infraestructura. |

## 4. Mapeo de fuentes al esquema común

| Fuente | Formato | Filas (líneas de producto) | Grano original |
|---|---|---|---|
| retail_orders | CSV | 707,000 | línea de producto |
| ecomm_orders | NDJSON | 404,000 (post-explode) | orden con líneas anidadas |
| app_orders | JSON | 202,000 | línea de producto |
| wholesale_orders | Parquet | 66,866 | agregado país/categoría/mes (B2B distribuidor) |
| franchise_orders | XLSX | 45,450 | línea de producto (fuente en español) |

**Esquema común (`fact_orders`):** `channel, consumer_id, order_id, country,
year_month, category, product_id, qty_ordered, qty_sold, unit_price,
currency, revenue_local, revenue_eur, unit_price_eur, order_status, adiclub`

## 5. Decisiones y asunciones documentadas

1. **País** — estandarizado a ISO2 (AR/BR/CL/CO/MX/PE). Cada fuente lo traía
   en formato distinto (nombre completo, ISO2, ISO3).

2. **Periodo** — estandarizado a `"YYYY-MM"`. Franchise venía con meses
   abreviados en español ("Abr-2025"), traducidos con diccionario explícito.

3. **Categoría de ecomm (`catCode` 1-4)** — la fuente no traía diccionario
   explícito. Se **infirió empíricamente** cruzando 240 SKUs compartidos
   entre `ecomm.article` y `app.productId` (sin overlap con wholesale, que
   usa un espacio de IDs distinto). Resultado: consistencia del 100% entre
   `catCode` y categoría de `app` en los productos cruzados.
   ```
   1 → Running   2 → Outdoor   3 → Football   4 → Originals
   ```

4. **Typos de categoría** — se detectaron errores de tipeo en `app`, `retail`
   y `franchise` (`Runing`, `Outdor`, `Footbal`, `Originls` mezclados con las
   formas correctas). Corregidos con diccionario de normalización. *Nota
   honesta: los typos de retail y franchise no se detectaron en el EDA
   inicial — se descubrieron durante la validación de la capa silver,
   revisando valores únicos de categoría post-transformación.*

5. **Moneda** — `ecomm` y `wholesale` llegan ya en EUR; `retail`, `app` y
   `franchise` en moneda local. La conversión se hace leyendo el valor real
   de la columna `currency` por fila (no se asume qué canal está en qué
   moneda), usando `currency_reference.csv` (join por país + mes).
   `EUR = monto_local / local_units_per_eur`.

6. **adiClub** — normalizado a booleano. En `app`, el 100% de los registros
   es `True` — hipótesis de negocio: la app requiere cuenta registrada para
   comprar, lo que asocia automáticamente al usuario a adiClub. Documentado
   como asunción razonable, no como certeza verificada con el negocio.

7. **Revenue en `app`** — el campo `amount` ya es el revenue total de la
   línea, calculado sobre `qty_sold` (no sobre `qty_ordered`); confirmado
   porque filas con `qty_sold = 0` tienen `amount = 0.0`. `unit_price` no
   viene directo en esta fuente, se deriva por división
   (`revenue_local / qty_sold`, protegido contra división por cero).

8. **Wholesale — sin `consumer_id` ni `order_id`** — es data reportada a
   nivel distribuidor (venta B2B), no a nivel de consumidor final. Se
   dejan estos campos como `NULL` explícito, en vez de generar valores
   sintéticos. **Consecuencia en KPIs:** wholesale participa en métricas de
   volumen/revenue (país, categoría, canal, mes), pero se excluye de
   cualquier métrica a nivel de cliente único.

9. **Wholesale — `revenue_local` nulo en 33,089 filas** — ocurre exactamente
   cuando `qty_sold = 0` (order_status `cancelled` o `processing`), porque
   la fuente no reporta `unit_price` sin una venta concretada. Se trata
   como revenue = 0 en las agregaciones, consistente con la definición de
   "venta real" basada en `qty_sold`.

10. **Estado de orden** — 4 estados consistentes en significado entre
    fuentes (`processing`, `delivered`, `cancelled`, `returned`), solo
    cambia idioma/capitalización. Franchise en español, normalizado con
    diccionario.

## 6. Definiciones de KPI (versionadas)

Todas las definiciones viven como funciones documentadas en `src/kpis.py`,
reutilizables para cualquier corte dimensional (país, canal, categoría, mes,
adiClub).

### KPI 1 — Revenue Bruto (v1.0)

```
Revenue Bruto = SUM(revenue_eur) WHERE order_status IN ('delivered', 'returned')
```

**Criterio de "venta real":** se considera venta real toda orden que
efectivamente generó una transacción (salió de inventario, se facturó), sin
importar si después fue devuelta. Se excluye `processing` (resultado aún
incierto) y `cancelled` (nunca hubo transacción efectiva). `revenue_eur`
nulo (wholesale sin venta concretada) se trata como 0.

### KPI 2 — Tasa de Devolución (v1.0)

```
Tasa de Devolución = SUM(revenue_eur WHERE returned) / Revenue Bruto
```

Mide qué proporción de la venta bruta terminó siendo revertida. Se calculó
por país, canal, categoría y mes — el resultado se mantuvo consistente en
todos los cortes (~12.4%-13.2%), sin outliers.

### KPI 3 — Tasa de Cumplimiento / Fulfillment Rate (v1.0)

```
Fulfillment Rate = SUM(qty_sold) / SUM(qty_ordered)
```

Mide qué proporción de la demanda pedida efectivamente se vendió/entregó.
No filtra por order_status (qty_sold ya es 0 en órdenes canceladas/en
proceso). Resultado consistente en todos los cortes (~77.5%-78.4%).

*Nota: las tasas 2 y 3 mostraron muy poca varianza entre país/canal/
categoría — consistente con que las cifras del ejercicio son sintéticas.
El patrón de negocio real se encontró en la dimensión adiClub (ver
sección 7), no en estas tasas.*

## 7. Insight + recomendación (executive readout)

**Hallazgo:** Los miembros de adiClub generan **65% más revenue total** que
los no-miembros (€192.6M vs €116.8M).

**Investigación de causa raíz:** se probó primero la hipótesis de "gastan
más por compra" — **descartada**: el ticket promedio por orden es
prácticamente idéntico entre grupos (€347.97 miembros vs €359.45
no-miembros). El driver real es **frecuencia de compra**: los miembros
generaron 553,504 órdenes vs 325,001 de no-miembros (**1.70x** en el
agregado; **1.22x-1.24x** de forma consistente en retail, ecommerce y
franchise individualmente — el agregado se ve más alto porque incluye
`app`, canal 100% adiClub sin contraparte no-miembro).

**Recomendación (call to action):** dado que el ticket promedio es
equivalente entre grupos, la oportunidad de negocio no está en upsell sino
en **crecer la base de miembros adiClub** (campañas de inscripción en
checkout/punto de venta) — cada conversión representa un incremento
predecible de frecuencia de compra, no un cambio de comportamiento de
gasto que haya que inducir.

**Cuantificación:** si el 10% de la base no-miembro migrara a la frecuencia
de compra observada en miembros, el revenue incremental estimado es
**€7.95M** (supuesto conservador y ajustable — sensibilidad discutible en
vivo si el panel pregunta por otros escenarios, ej. 5% o 20%).

## 8. Gobernanza y escalabilidad (plan)

- **Pipeline reproducible y versionable** — código en `src/` desacoplado de
  notebooks; agregar una 6ta fuente o renombrar un archivo existente no
  rompe la ingesta (bronze usa auto-descubrimiento por extensión, no
  nombres hardcodeados).
- **Definiciones de KPI centralizadas** — un solo lugar (`src/kpis.py`)
  define "venta real" y las métricas derivadas; cambios de criterio se
  versionan ahí, no se reinterpretan por reporte.
- **Adopción** — las tablas gold en Parquet se conectan directo a Power BI
  (herramienta ya estándar en el equipo), evitando imponer una herramienta
  nueva a analistas de negocio.
- **Stakeholders a involucrar:**
  - *Data Engineering* — mantenimiento del pipeline bronze/silver, futura
    migración a Databricks/Delta Lake si se decide productivizar.
  - *BI Team* — mantenimiento del dashboard Power BI y refresco periódico.
  - *Category/Channel Managers* — dueños de negocio de las definiciones de
    KPI (validar/ajustar criterio de "venta real" con reglas reales de
    Adidas, no solo el criterio inferido en este ejercicio).
  - *Loyalty/CRM team* — dueño natural del insight de adiClub, para diseñar
    la campaña de conversión de no-miembros.

## 9. Cómo reproducir

```bash
python -m venv venv
venv\Scripts\Activate.ps1          # Windows
pip install polars duckdb jupyter openpyxl pyarrow ipykernel

# Correr en orden:
# 1. notebooks/01_bronze_ingestion.ipynb
# 2. notebooks/02_silver_transformation.ipynb
# 3. notebooks/03_gold_kpis.ipynb
```

## 10. Limitaciones conocidas

- Las cifras son sintéticas (adaptadas para el ejercicio, según indica el
  brief) — el bajo nivel de varianza en Return Rate y Fulfillment Rate
  entre cortes es consistente con datos generados con probabilidad fija,
  no con un patrón de negocio insertado intencionalmente en esas métricas.
- El mapeo de `catCode` de ecomm es una inferencia estadística (100% de
  consistencia en la muestra cruzada), no una confirmación directa de una
  fuente autoritativa de catálogo de producto.
- La hipótesis de "app = 100% adiClub por requerir cuenta registrada" es
  una explicación de negocio razonable, no verificada contra documentación
  oficial del canal.

---
*Documento vivo — pendiente incorporar capturas del dashboard final y
ajustar el bloque 7 si cambian los supuestos de sensibilidad en la
presentación en vivo.*
