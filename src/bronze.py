"""
Capa Bronze — ingesta cruda y genérica de fuentes.
No transforma nombres de columnas ni valores: solo estandariza
el formato de almacenamiento a Parquet para trazabilidad y performance
en las siguientes capas.

Diseño: en vez de una función por fuente, una función parametrizada
que lee según la extensión del archivo, más un discovery automático
de qué fuentes existen en data/raw/.
"""

from pathlib import Path
from datetime import datetime, timezone
import polars as pl

RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")

# Mapea extensión de archivo -> función de lectura de Polars
READERS = {
    ".csv": pl.read_csv,
    ".ndjson": pl.read_ndjson,
    ".json": pl.read_json,
    ".parquet": pl.read_parquet,
    ".xlsx": pl.read_excel,
}


def discover_sources(raw_dir: Path = RAW_DIR) -> list[dict]:
    """
    Escanea data/raw/ y detecta automáticamente qué fuentes existen
    y de qué tipo son, basándose en la extensión del archivo.

    Devuelve una lista de dicts: [{"name": ..., "path": ..., "format": ...}, ...]
    Si aparece una fuente nueva, un archivo renombrado o un formato nuevo,
    esta función lo detecta sin necesidad de tocar código.
    """
    sources = []
    for f in sorted(raw_dir.iterdir()):
        if f.is_file() and f.suffix in READERS:
            sources.append({
                "name": f.stem,       # nombre del archivo sin extensión
                "path": f,
                "format": f.suffix,
            })
    return sources


def _add_ingestion_metadata(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    """Agrega metadata de trazabilidad: de dónde vino el dato y cuándo se ingestó."""
    return df.with_columns([
        pl.lit(source_name).alias("_source"),
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingested_at"),
    ])


def ingest_source(path: Path, name: str, file_format: str) -> pl.DataFrame:
    """
    Ingesta genérica: lee un archivo según su formato, agrega metadata,
    y lo guarda en bronze como Parquet.

    Parámetros son explícitos (no hardcodeados) para que la función
    sirva para cualquier fuente, actual o futura.
    """
    reader = READERS.get(file_format)
    if reader is None:
        raise ValueError(f"Formato no soportado: {file_format} (fuente: {name})")

    # read_csv y read_json a veces necesitan kwargs distintos; los manejamos aquí
    if file_format == ".csv":
        df = reader(path, infer_schema_length=10000)
    else:
        df = reader(path)

    df = _add_ingestion_metadata(df, source_name=path.name)

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(BRONZE_DIR / f"{name}.parquet")
    return df


def ingest_all(raw_dir: Path = RAW_DIR) -> dict[str, pl.DataFrame]:
    """
    Descubre todas las fuentes en raw_dir y las ingesta automáticamente.
    Si mañana se agrega una 6ta fuente o se renombra una existente,
    esta función no necesita cambios.
    """
    sources = discover_sources(raw_dir)
    if not sources:
        raise FileNotFoundError(f"No se encontraron fuentes soportadas en {raw_dir}")

    results = {}
    for src in sources:
        print(f"Ingestando: {src['name']}{src['format']} ...")
        results[src["name"]] = ingest_source(
            path=src["path"],
            name=src["name"],
            file_format=src["format"],
        )
    return results