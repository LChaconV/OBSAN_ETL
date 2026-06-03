import os
import pandas as pd
from pathlib import Path


def read_input_file(file_path: str) -> pd.DataFrame:
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Formato no soportado para mun_pdet: {suffix}")


def build_run_name(file_path: str) -> str:
    filename = Path(file_path).stem
    if "_run_" in filename:
        return f"run_{filename.split('_run_')[-1]}"
    return f"run_{filename}"


def run():
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
            raise ValueError("No se definió OBSAN_INPUT_FILE")
    df = read_input_file(file_path)
    df = df[["Código DANE Municipio"]].rename(columns={
        "Código DANE Municipio": "id_mun"
    })
    df["id_mun"] = df["id_mun"].astype(str).str.zfill(5)
    ruta = Path("data/golden/mun_pdet")
    ruta.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ruta / f"mun_pdet_{build_run_name(file_path)}.parquet", index=False)

if __name__ == "__main__":
    run()
