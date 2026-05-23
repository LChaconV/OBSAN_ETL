from __future__ import annotations

import logging
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import rioxarray
import xarray as xr
from rasterstats import zonal_stats

from src.etl.utils.logging_utils import setup_logging
from src.etl.utils.config_utils import load_yaml, save_yaml


# ============================================================
# RUTAS DEL PROYECTO
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "transform" / "terraclimate_transform.yaml"
STATE_PATH = PROJECT_ROOT / "state" / "state.yaml"
LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# CONFIGURACIÓN
# ============================================================

def load_transform_config() -> dict:
    config = load_yaml(CONFIG_PATH)

    if "terraclimate_transform" not in config:
        raise KeyError("No existe la clave 'terraclimate_transform' en el YAML.")

    return config["terraclimate_transform"]


def load_full_state() -> dict:
    if not STATE_PATH.exists():
        return {}

    return load_yaml(STATE_PATH)


def save_full_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    save_yaml(STATE_PATH, state)


# ============================================================
# STATE TRANSFORM
# ============================================================

def get_pending_years(state: dict, variable: str, start_year: int) -> list[int]:
    extracted_until = (
        state
        .get("terraclimate", {})
        .get("variables", {})
        .get(variable, {})
        .get("last_incremental_value")
    )

    transformed_until = (
        state
        .get("terraclimate_transform", {})
        .get("variables", {})
        .get(variable, {})
        .get("last_transformed_value")
    )

    if extracted_until is None:
        return []

    start = int(transformed_until) + 1 if transformed_until is not None else start_year
    end = int(extracted_until)

    if start > end:
        return []

    return list(range(start, end + 1))


def update_transform_state(state: dict, variable: str, year: int) -> dict:
    state.setdefault("terraclimate_transform", {})
    state["terraclimate_transform"].setdefault("variables", {})
    state["terraclimate_transform"]["variables"].setdefault(variable, {})

    state["terraclimate_transform"]["variables"][variable] = {
        "last_transformed_value": int(year),
        "last_transformed_at": pd.Timestamp.utcnow().isoformat(),
        "status": "transformed",
    }

    return state


# ============================================================
# MUNICIPIOS
# ============================================================

def load_municipalities(config: dict) -> gpd.GeoDataFrame:
    path = PROJECT_ROOT / config["source"]["municipalities_path"]

    if not path.exists():
        raise FileNotFoundError(f"No existe la capa municipal: {path}")

    gdf = gpd.read_parquet(path)

    municipality_id = config["columns"]["municipality_id"]

    if municipality_id not in gdf.columns:
        raise ValueError(f"No existe la columna municipal requerida: {municipality_id}")

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    gdf = gdf.to_crs("EPSG:4326")

    return gdf[[municipality_id, "geometry"]].copy()


# ============================================================
# NETCDF
# ============================================================

def open_terraclimate_file(config: dict, variable: str, year: int) -> xr.DataArray:
    bronze_dir = PROJECT_ROOT / config["source"]["bronze_dir"]
    file_path = bronze_dir / variable / f"TerraClimate_{variable}_{year}.nc"

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el NetCDF: {file_path}")

    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("bronze_dir:", bronze_dir)
    print("file_path:", file_path)
    print("exists:", file_path.exists())
    ds = xr.open_dataset(
    file_path,
    engine="netcdf4",
    mask_and_scale=True,
    decode_times=True,
    )

    if variable not in ds.data_vars:
        raise ValueError(f"La variable '{variable}' no existe en {file_path}")

    da = ds[variable]

    if "lon" in da.dims and "lat" in da.dims:
        da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)

    da = da.rio.write_crs("EPSG:4326", inplace=False)

    return da


def clip_to_municipality_bounds(
    da: xr.DataArray,
    municipalities: gpd.GeoDataFrame,
) -> xr.DataArray:
    minx, miny, maxx, maxy = municipalities.total_bounds

    return da.rio.clip_box(
        minx=minx,
        miny=miny,
        maxx=maxx,
        maxy=maxy,
    )


# ============================================================
# RASTER TEMPORAL Y ESTADÍSTICA ZONAL
# ============================================================

def save_month_raster(
    da_month: xr.DataArray,
    tmp_dir: Path,
    variable: str,
    year: int,
    month: int,
) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)

    raster_path = tmp_dir / f"{variable}_{year}_{month:02d}.tif"

    # Importante: evita problemas con scale_factor y enteros crudos.
    da_month = da_month.astype("float32")

    da_month.rio.to_raster(raster_path)

    return raster_path


def calculate_monthly_zonal_stats(
    municipalities: gpd.GeoDataFrame,
    raster_path: Path,
    municipality_id: str,
    stat: str,
) -> pd.DataFrame:
    stats = zonal_stats(
        municipalities,
        raster_path,
        stats=[stat],
        nodata=None,
        geojson_out=False,
    )

    result = municipalities[[municipality_id]].copy()
    result["value"] = [item.get(stat) for item in stats]

    return result


# ============================================================
# SILVER MENSUAL POR VARIABLE-AÑO
# ============================================================

def build_monthly_for_variable_year(
    config: dict,
    municipalities: gpd.GeoDataFrame,
    variable: str,
    year: int,
) -> pd.DataFrame:
    municipality_id = config["columns"]["municipality_id"]
    tmp_dir = PROJECT_ROOT / config["temporary"]["raster_dir"]

    monthly_stat = config["variables"][variable]["monthly_stat"]

    da = open_terraclimate_file(config, variable, year)
    da = clip_to_municipality_bounds(da, municipalities)

    monthly_results = []

    for time_index in range(da.sizes["time"]):
        da_month = da.isel(time=time_index)

        timestamp = pd.to_datetime(str(da_month["time"].values))
        month = int(timestamp.month)

        raster_path = save_month_raster(
            da_month=da_month,
            tmp_dir=tmp_dir,
            variable=variable,
            year=year,
            month=month,
        )

        monthly_df = calculate_monthly_zonal_stats(
            municipalities=municipalities,
            raster_path=raster_path,
            municipality_id=municipality_id,
            stat=monthly_stat,
        )

        monthly_df["year"] = year
        monthly_df["month"] = month
        monthly_df["variable"] = variable
        monthly_df["statistic"] = monthly_stat

        monthly_df = monthly_df.rename(columns={municipality_id: "id_mun"})

        monthly_results.append(monthly_df)

    if not monthly_results:
        return pd.DataFrame()

    monthly_df = pd.concat(monthly_results, ignore_index=True)

    monthly_df = monthly_df[
        [
            "id_mun",
            "year",
            "month",
            "variable",
            "statistic",
            "value",
        ]
    ]

    return monthly_df


# ============================================================
# GOLDEN ANUAL POR VARIABLE-AÑO
# ============================================================

def build_variable_annual(monthly_df: pd.DataFrame, annual_agg: str) -> pd.DataFrame:
    if monthly_df.empty:
        return pd.DataFrame()

    if annual_agg == "sum":
        annual_df = (
            monthly_df
            .groupby(["id_mun", "year", "variable"], as_index=False)["value"]
            .sum()
        )
    elif annual_agg == "mean":
        annual_df = (
            monthly_df
            .groupby(["id_mun", "year", "variable"], as_index=False)["value"]
            .mean()
        )
    else:
        raise ValueError(f"Agregación anual no soportada: {annual_agg}")

    annual_df["annual_aggregation"] = annual_agg

    annual_df = annual_df[
        [
            "id_mun",
            "year",
            "variable",
            "annual_aggregation",
            "value",
        ]
    ]

    return annual_df


# ============================================================
# GUARDADO
# ============================================================

def save_variable_outputs(
    monthly_df: pd.DataFrame,
    annual_df: pd.DataFrame,
    variable: str,
    year: int,
    config: dict,
) -> None:
    silver_dir = PROJECT_ROOT / config["output"]["silver_dir"]
    golden_dir = PROJECT_ROOT / config["output"]["golden_dir"]
    file_prefix = config["output"]["file_prefix"]

    silver_dir.mkdir(parents=True, exist_ok=True)
    golden_dir.mkdir(parents=True, exist_ok=True)

    monthly_path = silver_dir / f"{file_prefix}_{variable}_monthly_{year}.parquet"
    annual_path = golden_dir / f"{file_prefix}_{variable}_annual_{year}.parquet"

    monthly_df.to_parquet(monthly_path, index=False)
    annual_df.to_parquet(annual_path, index=False)

    logging.info("Silver mensual guardado: %s", monthly_path)
    logging.info("Golden anual guardado: %s", annual_path)


def save_error(error_records: list[dict], config: dict) -> None:
    if not error_records:
        return

    error_dir = PROJECT_ROOT / config["output"]["error_dir"]
    error_dir.mkdir(parents=True, exist_ok=True)

    error_path = error_dir / "terraclimate_errors.parquet"

    error_df = pd.DataFrame(error_records)

    if error_path.exists():
        previous = pd.read_parquet(error_path)
        error_df = pd.concat([previous, error_df], ignore_index=True)

    error_df.to_parquet(error_path, index=False)

    logging.info("Errores guardados en: %s", error_path)


# ============================================================
# MAIN
# ============================================================

def run() -> None:
    setup_logging(LOG_DIR, "terraclimate_transform.log")
    logging.info("Iniciando transformación incremental TerraClimate")

    config = load_transform_config()
    full_state = load_full_state()

    municipalities = load_municipalities(config)

    start_year = config["years"]["start_year"]

    total_processed = 0
    error_records = []

    for variable, var_cfg in config["variables"].items():
        pending_years = get_pending_years(
            state=full_state,
            variable=variable,
            start_year=start_year,
        )

        if not pending_years:
            logging.info(
                "No hay años pendientes para transformar. variable=%s",
                variable,
            )
            continue

        logging.info(
            "Años pendientes para variable=%s: %s",
            variable,
            pending_years,
        )

        for year in pending_years:
            try:
                logging.info(
                    "Transformando TerraClimate variable=%s año=%s",
                    variable,
                    year,
                )

                monthly_df = build_monthly_for_variable_year(
                    config=config,
                    municipalities=municipalities,
                    variable=variable,
                    year=year,
                )

                annual_df = build_variable_annual(
                    monthly_df=monthly_df,
                    annual_agg=var_cfg["annual_agg"],
                )

                save_variable_outputs(
                    monthly_df=monthly_df,
                    annual_df=annual_df,
                    variable=variable,
                    year=year,
                    config=config,
                )

                full_state = update_transform_state(
                    state=full_state,
                    variable=variable,
                    year=year,
                )

                save_full_state(full_state)

                total_processed += 1

                logging.info(
                    "Transformación completada. variable=%s año=%s",
                    variable,
                    year,
                )

            except Exception as e:
                logging.exception(
                    "Error transformando variable=%s año=%s. "
                    "Se detiene esta variable y no se procesarán años posteriores.",
                    variable,
                    year,
                )

                error_records.append(
                    {
                        "variable": variable,
                        "year": year,
                        "error": str(e),
                        "error_at": pd.Timestamp.utcnow().isoformat(),
                        "action": "variable_stopped",
                    }
                )

                save_error(error_records, config)

                break

    if total_processed == 0:
        logging.info("No se transformaron nuevos archivos TerraClimate.")
    else:
        logging.info("Total variable-año transformados: %s", total_processed)

    logging.info("Transformación incremental TerraClimate finalizada.")


if __name__ == "__main__":
    run()