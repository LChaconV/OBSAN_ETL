from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
import yaml
import pandas as pd

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, save_yaml
from src.etl.utils.transform_utils import ensure_five_digits

PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "censo_pecuario_transform.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


def load_transform_config(key: str) -> dict:
    config = load_yaml(CONFIG_PATH)

    if key not in config:
        raise KeyError(f"No existe la clave '{key}' en censo_pecuario_transform.yaml")
    return config[key]


def get_runtime_params() -> tuple[Path, int, str]:
    #input_file= Path(r"C:\Users\laura\ESCUELA COLOMBIANA DE INGENIERIA JULIO GARAVITO\Proyecto OBSAN - General\Datos_OBSAN_web\OBSAN\observatorio-san\data\bronze\censo_pecuario\run_2026_04_27\CENSOS-EQUINOS-2023-Final.xls")
    #ear="2025"
    #animal_type="equino" 
    input_file = os.environ.get("OBSAN_INPUT_FILE")
    year = os.environ.get("OBSAN_YEAR")
    animal_type = os.environ.get("OBSAN_ANIMAL_TYPE")

    if not input_file:
        raise ValueError("Falta la variable de entorno OBSAN_INPUT_FILE")

    if not year:
        raise ValueError("Falta la variable de entorno OBSAN_YEAR")

    if not animal_type:
        raise ValueError("Falta la variable de entorno OBSAN_ANIMAL_TYPE")

    file_path = Path(input_file)

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {file_path}")

    return file_path, int(year), animal_type.strip().lower()


def find_header_row_excel(file_path: Path, required_columns: list, max_rows: int = 30) -> int:
    preview = pd.read_excel(file_path, header=None, nrows=max_rows)
    normalized = normalize_required_columns(required_columns)

    for idx, row in preview.iterrows():
        row_cols_upper = {str(v).strip().upper(): str(v) for v in row.dropna()}
        if all(
            find_matching_column(group, row_cols_upper) is not None
            for group in normalized
        ):
            return idx

    raise ValueError(
        f"No se encontró encabezado con las columnas requeridas: {required_columns}"
    )


def read_input_file(file_path, required_columns: list[str]) -> pd.DataFrame:
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    if suffix in [".xlsx", ".xls"]:
        header_row = find_header_row_excel(file_path, required_columns)
        logging.info("Fila de encabezado detectada: %s", header_row)
        return pd.read_excel(file_path, header=header_row)

    if suffix == ".csv":
        return pd.read_csv(file_path)

    if suffix == ".parquet":
        return pd.read_parquet(file_path)

    raise ValueError(f"Formato no soportado: {suffix}")


def validate_required_columns(df: pd.DataFrame, config: dict) -> None:
    normalized = normalize_required_columns(config["validation"]["required_columns"])
    df_cols_upper = {col.strip().upper(): col for col in df.columns}

    missing = [
        group for group in normalized
        if find_matching_column(group, df_cols_upper) is None
    ]

    if missing:
        raise ValueError(f"Faltan columnas requeridas (ningún alias encontrado): {missing}")


def transform_censo_pecuario(
    df: pd.DataFrame,
    config: dict,
    year: int,
    animal_type: str,
) -> pd.DataFrame:
    fact_cfg = config["fact_table"]
    alias_rename = resolve_column_aliases(df, config["validation"]["required_columns"])
    if alias_rename:
        logging.info("Aliases resueltos: %s", alias_rename)
        df = df.rename(columns=alias_rename)

    rename_columns = fact_cfg["rename_columns"]
    grain = fact_cfg["grain"]
    metric_columns = fact_cfg["metric_columns"]

    df = df.rename(columns=rename_columns)
    df= ensure_five_digits(df, "id_mun")
    df= df.dropna(subset=["id_mun"])

    df["year"] = year
    df["type"] = animal_type

    df["id_mun"] = df["id_mun"].astype(str).str.strip()

    for col in metric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    group_columns = grain + ["type"]

    df_gold = (
        df.groupby(group_columns, dropna=False, as_index=False)[metric_columns]
        .sum()
    )
    if "total_farms" not in df_gold.columns:
        logging.warning("Columna 'total_farms' no encontrada. Asignando valores nulos.")
        df_gold["total_farms"] = 0

    return df_gold.sort_values(group_columns).reset_index(drop=True)


def save_golden(df: pd.DataFrame, config: dict, year: int, animal_type: str) -> Path:
    golden_dir = PROJECT_ROOT / config["source"]["golden_fact_dir"]
    golden_dir.mkdir(parents=True, exist_ok=True)
    df["id_mun"] = df["id_mun"].astype(str).str.strip()
    output_path = golden_dir / f"censo_{animal_type}_{year}.parquet"
    df.to_parquet(output_path, index=False)

    logging.info("Archivo golden guardado en: %s", output_path)

    return output_path
def normalize_required_columns(required_columns: list) -> list[list[str]]:
    """Normaliza cada entrada a lista de aliases. Un string queda como lista de uno."""
    return [
        col if isinstance(col, list) else [col]
        for col in required_columns
    ]

def resolve_column_aliases(df: pd.DataFrame, required_columns: list) -> dict:
    """
    Devuelve un rename dict {col_real_en_df -> nombre_canonico}
    para columnas que llegaron con un alias distinto al canónico.
    El nombre canónico es el primer elemento de cada grupo.
    """
    normalized = normalize_required_columns(required_columns)
    rename = {}
    df_cols_upper = {col.strip().upper(): col for col in df.columns}

    for group in normalized:
        canonical = group[0]
        match = find_matching_column(group, df_cols_upper)
        if match and match != canonical:
            rename[match] = canonical

    return rename

def find_matching_column(aliases: list[str], df_cols_upper: dict[str, str]) -> str | None:
    """
    Busca en df_cols_upper la primera columna que coincida con algún alias.
    Estrategia: exacto primero, luego startswith, luego contains.
    Devuelve el nombre real de la columna en el df, o None si no hay match.
    """
    for strategy in ("exact", "startswith", "contains"):
        for alias in aliases:
            alias_up = alias.strip().upper()
            for col_up, col_real in df_cols_upper.items():
                if strategy == "exact"      and col_up == alias_up:
                    return col_real
                if strategy == "startswith" and col_up.startswith(alias_up):
                    return col_real
                if strategy == "contains"   and alias_up in col_up:
                    return col_real
    return None

def run() -> None:
    file_path, year, animal_type = get_runtime_params()
    key = animal_type

    setup_logging(LOG_DIR, f"censo_{key}_transform.log")
    logging.info("Iniciando transformación de censo pecuario - %s", key)

    config = load_transform_config(key)

    logging.info("Archivo recibido: %s", file_path)
    logging.info("Año recibido: %s", year)
    logging.info("Tipo recibido: %s", animal_type)

    required_columns = config["validation"]["required_columns"]

    df = read_input_file(file_path, required_columns)
    print (df.head())

    logging.info("Filas leídas: %s", len(df))
    logging.info("Columnas leídas: %s", df.columns.tolist())

    validate_required_columns(df, config)
    
    df_gold = transform_censo_pecuario(
        df=df,
        config=config,
        year=year,
        animal_type=animal_type,
    )


    logging.info("Filas finales golden: %s", len(df_gold))

    for i in df_gold:
        print (i)

    save_golden(
        df=df_gold,
        config=config,
        year=year,
        animal_type=animal_type,
    )
    full_config = load_yaml(CONFIG_PATH)
    full_config[key]["source"]["last_file_transformed"] = f"censo_{animal_type}_{year}.parquet"
    save_yaml(CONFIG_PATH, full_config)

    logging.info("Transformación de %s finalizada correctamente", key)


if __name__ == "__main__":
    run()