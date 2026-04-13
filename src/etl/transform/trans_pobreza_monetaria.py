import pandas as pd
from pathlib import Path
import yaml

with open("config/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)


def add_department_code(df, dept_col="departamento", code_col="id_dept"):
    
    # diccionario de departamentos
    dept_codes = {
        "Antioquia": "05",
        "Atlántico": "08",
        "Bogotá D.C.": "11",
        "Bolívar": "13",
        "Boyacá": "15",
        "Caldas": "17",
        "Caquetá": "18",
        "Cauca": "19",
        "Cesar": "20",
        "Córdoba": "23",
        "Chocó": "27",
        "Cundinamarca": "25",
        "Huila": "41",
        "La Guajira": "44",
        "Magdalena": "47",
        "Meta": "50",
        "Nariño": "52",
        "Norte de Santander": "54",
        "Quindío": "63",
        "Risaralda": "66",
        "Santander": "68",
        "Sucre": "70",
        "Tolima": "73",
        "Valle del Cauca": "76"
    }

    # crear columna con el código
    df[code_col] = df[dept_col].map(dept_codes)

    # advertencia si algún departamento no coincide
    missing = df[df[code_col].isna()][dept_col].unique()
    
    if len(missing) > 0:
        print("Advertencia: departamentos sin código encontrado:")
        print(missing)

    return df

base = Path(config["silver"]["pme_dir"])
file = config["silver"]["pme_sex_p_file"]
path = base / file
df = pd.read_csv(path)
df = add_department_code(df)
df.to_csv(path, index=False)

file = config["silver"]["pme_sex_h_file"]
path = base / file
df = pd.read_csv(path)
df = add_department_code(df)
df.to_csv(path, index=False)
