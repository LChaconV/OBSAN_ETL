from . import  transform, load

def run(**kwargs):

    print("Iniciando transformación...")
    transform.run()
    print("Transformación completada")

    print("Iniciando carga...")
    load.run()
    print("Carga completada.")

if __name__ == "__main__":
    run()