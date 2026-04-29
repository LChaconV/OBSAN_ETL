# runner.py
import importlib

def run_pipeline(source_name: str, **kwargs):
    module = importlib.import_module(f"src.etl.{source_name}.pipeline")
    module.run(**kwargs)

# Para correrlo manualmente desde terminal:
# python runner.py fuente_a
if __name__ == "__main__":
    import sys
    run_pipeline(sys.argv[1])