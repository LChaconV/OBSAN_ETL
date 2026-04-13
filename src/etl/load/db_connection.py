from sqlalchemy import create_engine

def get_engine():
 
    USER = "postgres"
    PASS = "tu_password"
    HOST = "localhost"
    DB = "observatorio_san"
    
    url = f"postgresql://{USER}:{PASS}@{HOST}:5432/{DB}"
    return create_engine(url)