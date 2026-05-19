# OBSAN - Pipeline ETL

Este proyecto implementa un pipeline ETL para el Observatorio de Seguridad Alimentaria. El flujo general es:

```text
Extract → Bronze → Transform → Silver/Golden → Load → PostgreSQL
```
## Base de datos

Configurar conexión en:
```text
config/db.yaml
```
Con la siguiente estructura:

```yaml
postgresql:
  user: ""       # Usuario de PostgreSQL. Ejemplo: "postgres"
  
  pass: ""       # Contraseña del usuario de PostgreSQL
  
  host: ""       # Dirección del servidor PostgreSQL. Ejemplo: "localhost"
  
  port:          # Puerto de PostgreSQL. Normalmente: 5432
  
  database: ""   # Nombre de la base de datos del proyecto. Ejemplo: "observatorio"

# Parámetros opcionales de rendimiento
pooling:
  min_size: 1    # Número mínimo de conexiones mantenidas en el pool
  max_size: 5    # Número máximo de conexiones simultáneas
```

# Guía de Configuración y Carga de Archivos OBSAN Web

Tras completar la configuración inicial de la base de datos, proceda con los siguientes pasos para clonar el repositorio y cargar la información geoespacial requerida.

## 1. Clonación del Repositorio

Descargue el código fuente del proyecto ejecutando el siguiente comando en su terminal o descargándolo directamente desde el enlace:

* **Repositorio:** [OBSAN_Web en GitHub](https://github.com/LChaconV/OBSAN_Web.git)

---

## 2. Instrucciones para la Carga de Archivos

1. Acceda a la interfaz de la aplicación web.
2. En la **barra lateral izquierda**, busque y seleccione la opción **"Carga de Archivos"**.
3. Cargue los archivos correspondientes a las variables detalladas a continuación:

### Variable 1: División municipal de Colombia
* **Descripción:** Archivo geográfico de la división por municipios.
* **Enlace de descarga:** [Descargar Archivo 1 (SharePoint)](https://pruebacorreoescuelaingeduco.sharepoint.com/:u:/s/ProyectoOBSAN/IQCfLTsb3GruTbeFRmpQA9iPAZkXWVYt9IKp8k8ef330YjQ?e=uo1At5)

### Variable 2: División departamental de Colombia
* **Descripción:** Archivo geográfico de la división por departamentos.
* **Enlace de descarga:** [Descargar Archivo 2 (SharePoint)](https://pruebacorreoescuelaingeduco.sharepoint.com/:u:/s/ProyectoOBSAN/IQBkbzU5gX6mT6dnRh_2uy1AAaGqmXfjOgBf1s8GETGloAg?e=WUSqs3)

