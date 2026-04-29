from . import extract, transform, load

def run(**kwargs):
    print("Iniciando extracción...")
    run_dir =extract.run(**kwargs)
    print("Extracción completada")

    if run_dir is None:
        print("No hay datos nuevos. Se omite transformación y carga.")
        return

    print("Iniciando transformación...")
    transform.run()
    print("Transformación completada")

    print("Iniciando carga...")
    load.run()
    print("Carga completada.")

if __name__ == "__main__":
    run()