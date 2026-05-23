from . import  extract,transform, load

def run(**kwargs):

    print("Iniciando extracción...")
    extract.run()
    print("Extracción completada")

    print("Iniciando transformación...")
    transform.run()
    print("Transformación completada")

    print("Iniciando carga...")
    load.run()
    print("Carga completada.")

if __name__ == "__main__":
    run()