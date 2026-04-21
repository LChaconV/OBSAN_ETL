import pandas as pd
from pathlib import Path
# 1. Leer el archivo Excel
df = pd.read_excel(r"data\bronze\mun_pdet\MunicipiosPDET.xlsx")

# 2. Seleccionar y renombrar la columna
df = df[["Código DANE Municipio"]].rename(columns={
    "Código DANE Municipio": "id_mun"
})

# (opcional) asegurar formato tipo string con ceros a la izquierda
df["id_mun"] = df["id_mun"].astype(str).str.zfill(5)


ruta = Path("data/golden/mun_pdet")

# Crea la carpeta si no existe (incluye subcarpetas)
ruta.mkdir(parents=True, exist_ok=True)
df.to_parquet(ruta / "mun_pdet.parquet", index=False)