import os
import pandas as pd
from pathlib import Path

def run():
    file_path = os.environ.get("OBSAN_INPUT_FILE")
    if not file_path:
            raise ValueError("No se definió OBSAN_INPUT_FILE")
    df = pd.read_excel(file_path)
    df = df[["Código DANE Municipio"]].rename(columns={
        "Código DANE Municipio": "id_mun"
    })
    df["id_mun"] = df["id_mun"].astype(str).str.zfill(5)
    ruta = Path("data/golden/mun_pdet")
    ruta.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ruta / "mun_pdet.parquet", index=False)

if __name__ == "__main__":
    run()

