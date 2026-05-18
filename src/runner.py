# runner.py
import importlib
import sys
def run_pipeline(source_name: str, **kwargs):
    module = importlib.import_module(f"src.etl.{source_name}.pipeline")
    module.run(**kwargs)

# Para correrlo manualmente desde terminal:
# python -m src.runner "fuente_a"
if __name__ == "__main__":
    run_pipeline(sys.argv[1])
    #run_pipeline("api_beneficiarios_iraca")