from . import transform, load
from src.etl.utils.pipeline_cleanup import run_with_cleanup


def _run_steps(**kwargs):
    print("Iniciando transformación...")
    transform.run()
    print("Transformación completada")
    print("Iniciando carga...")
    load.run()
    print("Carga completada.")


def run(**kwargs):
    return run_with_cleanup(__name__, _run_steps, **kwargs)
