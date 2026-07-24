# OBSAN Geovisor — Guía de instalación y ejecución

Repositorio unificado del Observatorio de Seguridad Alimentaria y Nutricional (OBSAN): el proceso ETL que recolecta y prepara los datos, y el Geovisor, la aplicación web en Streamlit que los visualiza.

Esta guía cubre todo lo necesario para levantar el sistema desde cero y terminar con un Geovisor con datos reales cargados, no solo con la aplicación corriendo vacía. Léela en orden: la sección "Cargar los datos" es la que realmente determina si el mapa se ve completo o vacío.

## Requisitos previos

- Python 3.11
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (gestor de entorno y dependencias del proyecto)
- PostgreSQL 16 con la extensión PostGIS habilitada
- Docker y Docker Compose (opcional, pero es el camino más simple para tener base de datos + app funcionando sin instalar Postgres a mano)
- Git

## Estructura principal del repositorio

```text
Geovisor_OBSAN/
├── apps/
│   └── streamlit/
│       ├── Geovisor.py          ← página principal (mapa)
│       ├── pages/                ← hojas adicionales (carga de archivos, ejecuciones ETL, análisis temporal)
│       ├── components/, core/, config/, upload/
├── src/
│   ├── runner.py                 ← ejecuta un pipeline puntual desde consola
│   ├── scheduler.py               ← programador de ejecuciones automáticas
│   └── etl/
│       ├── api_*/                ← fuentes con extracción automática (API pública)
│       ├── dw_*/                  ← capas de referencia territorial
│       ├── ld_*/                  ← fuentes de carga manual (archivo)
│       ├── url_terraclimate/      ← fuente climática automática
│       └── utils/
├── config/                        ← configuración de extracción y transformación
├── sql/                            ← vistas y seeds SQL de apoyo
├── docker/, Dockerfile, docker-compose.prod.yml
├── ecosystem.config.js            ← agenda del scheduler bajo PM2
├── pyproject.toml, uv.lock
└── data/obligatory/divipola.parquet   ← única capa de datos que viene incluida en el repo
```

## Camino recomendado: todo con Docker Compose

Es la forma más simple de tener base de datos, aplicación web y programador de ETL corriendo juntos, sin instalar PostgreSQL manualmente.

```bash
git clone <url-del-repositorio>
cd Geovisor_OBSAN

cp .env.production.example .env.production
# Edita .env.production y cambia DB_PASSWORD / POSTGRES_PASSWORD por una clave propia

docker compose -f docker-compose.prod.yml up -d --build
```

Esto levanta tres servicios:

| Servicio    | Qué hace                                                                 | Puerto  |
|-------------|---------------------------------------------------------------------------|---------|
| `db`        | PostgreSQL 16 + PostGIS, con las extensiones ya habilitadas al iniciar   | 5432    |
| `web`       | La aplicación Streamlit (Geovisor)                                       | 8501    |
| `scheduler` | Ejecuta automáticamente los pipelines de fuentes públicas, según horario | —       |

Abre `http://localhost:8501`. La aplicación va a cargar, pero **el mapa estará vacío** hasta completar la sección "Cargar los datos" más abajo — levantar los contenedores solo prepara la infraestructura, no trae datos por sí solo (salvo lo que el `scheduler` empiece a traer automáticamente con el tiempo).

## Camino alternativo: entorno local sin Docker

Útil para desarrollo, si prefieres tener PostgreSQL instalado directamente en tu máquina.

```bash
# 1. Crear la base de datos y habilitar PostGIS (usando psql)
psql -U postgres -c "CREATE DATABASE obsan;"
psql -U postgres -d obsan -c "CREATE EXTENSION IF NOT EXISTS postgis;"
psql -U postgres -d obsan -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"

# 2. Instalar dependencias del proyecto
uv sync

# 3. Configurar variables de entorno
cp .env.production.example .env
# Edita .env: DB_HOST=localhost y las credenciales que hayas usado en el paso 1

# 4. Ejecutar la aplicación
uv run streamlit run apps/streamlit/Geovisor.py
```

No hace falta ejecutar migraciones aparte: cada pipeline crea sus propias tablas la primera vez que corre (`CREATE TABLE IF NOT EXISTS`), como parte de la carga.

## Cargar los datos: cómo obtener un Geovisor completamente funcional

Esta es la parte que de verdad "llena" el mapa. Se hace en tres pasos, en este orden.

### Paso 1 — Referencia geográfica (obligatorio, antes que cualquier otra cosa)

Todas las demás capas se relacionan con el territorio a través de los códigos de municipio y departamento. Sin este paso, ninguna otra capa va a mostrar nada así se cargue correctamente.

Divipola (municipios) ya viene incluido en el repositorio como archivo parquet, así que se carga directo:

```bash
OBSAN_INPUT_FILE=data/obligatory/divipola.parquet uv run -m src.runner "dw_divipola"
```

Departamentos no viene incluido: descarga el archivo GeoJSON desde `https://www.colombiaenmapas.gov.co/` (división departamental de Colombia) y cárgalo:

```bash
OBSAN_INPUT_FILE=/ruta/al/archivo/departamentos.geojson uv run -m src.runner "dw_departamento"
```

### Paso 2 — Fuentes automáticas (conectadas a una API pública)

Estas fuentes no requieren ningún archivo: se conectan solas a portales de datos abiertos. Si dejas el `scheduler` corriendo (incluido por defecto en Docker Compose), se van a traer solas según el horario definido en `ecosystem.config.js`. Si no quieres esperar, puedes dispararlas una por una:

```bash
uv run -m src.runner "api_edu_escolar"
uv run -m src.runner "api_edu_superior"
uv run -m src.runner "api_erradicacion_cultivos_coca"
uv run -m src.runner "api_familias_accion"
uv run -m src.runner "api_indice_riesgo_irca"
uv run -m src.runner "api_minerales"
uv run -m src.runner "api_oro_aluvion"
uv run -m src.runner "api_produc_gas"
uv run -m src.runner "api_produc_petroleo"
uv run -m src.runner "api_regalias"
uv run -m src.runner "api_victimas"
uv run -m src.runner "url_terraclimate"
```

`url_terraclimate` tarda considerablemente más que las demás (descarga varios años de datos climáticos satelitales), ten paciencia con esa.

Para dejar el scheduler corriendo de forma continua en lugar de disparar cada una a mano:

```bash
uv run -m src.scheduler
# o, con PM2 (igual a como corre en producción):
pm2 start ecosystem.config.js
```

### Paso 3 — Fuentes manuales (vía la interfaz web)

Con la aplicación corriendo, entra a la hoja **Carga de Archivos** y sube, una por una, las variables que necesites. Cada formulario en esa página muestra el formato esperado y, cuando existe, un enlace de descarga directo a la fuente pública. Las variables disponibles en esta página son:

Población, Mercado laboral, Mortalidad por desnutrición, Desnutrición aguda en menores de 5 años, Bajo peso al nacer, Mercados campesinos, Censo pecuario (requiere indicar año y especie), Incidencia de Pobreza Multidimensional, Pobreza monetaria municipal, Coeficiente de GINI, Necesidades Básicas Insatisfechas (requiere indicar año), Producción Agrícola, Resguardo indígena.

La mayoría provienen de descargas públicas del DANE, SIVIGILA, ICA o UPRA; el formulario de cada variable indica la fuente exacta.

### Variables de uso interno (no aparecen en la interfaz de carga)

Municipios PDET y Subregiones de Antioquia están marcadas como ocultas a propósito (son capas de referencia que casi nunca cambian) y solo se cargan por consola:

```bash
OBSAN_INPUT_FILE=/ruta/al/archivo/municipios_pdet.xlsx uv run -m src.runner "dw_mun_pdet"
OBSAN_INPUT_FILE=/ruta/al/archivo/subregiones_antioquia.geojson uv run -m src.runner "dw_subregiones_antioquia"
```

### Dato que no es de descarga pública

La capa central del observatorio, **Inseguridad alimentaria (Perfil Antioquia)**, depende de los resultados de una encuesta propia de la Universidad de Antioquia que no se descarga de un portal público. Sin ese archivo, específicamente esa capa quedará vacía aunque el resto del sistema funcione con normalidad; consíguelo con el equipo del observatorio y cárgalo desde la hoja de Carga de Archivos (variable "Perfil alimentario Antioquia").

## Verificar que quedó funcionando

- La aplicación carga en `http://localhost:8501` sin errores de conexión a la base de datos (el ícono de estado en la barra lateral debe mostrarse en verde).
- Al activar la capa "Municipios" o "Departamentos" en el mapa, se ven los polígonos del territorio (confirma que el Paso 1 quedó bien).
- Al activar alguna capa de la categoría Hidrocarburos o Educación, aparecen íconos o burbujas sobre el mapa (confirma que al menos una fuente automática cargó datos).
- La hoja "Ejecuciones ETL" muestra corridas registradas, no una lista vacía.

## Variables de entorno

| Variable       | Uso                                                              | Obligatoria |
|----------------|-------------------------------------------------------------------|-------------|
| `DB_HOST`      | Host de PostgreSQL                                                | Sí          |
| `DB_PORT`      | Puerto de PostgreSQL (por defecto 5432)                          | No          |
| `DB_NAME`      | Nombre de la base de datos                                        | Sí          |
| `DB_USER`      | Usuario de PostgreSQL                                             | Sí          |
| `DB_PASSWORD`  | Contraseña del usuario                                            | Sí          |
| `SOCRATA_APP_TOKEN` | Token opcional para las APIs de datos.gov.co (evita límites de tasa) | No |
| `OBSAN_INPUT_FILE`  | Ruta del archivo a procesar en pipelines de carga manual ejecutados por consola | Solo para esos pipelines |

## Solución de problemas comunes

**"Falta la variable de entorno obligatoria: DB_HOST" (u otra similar)**: falta el archivo `.env` (entorno local) o `.env.production` (Docker), o no tiene todas las variables de la tabla anterior.

**El mapa carga pero ninguna capa muestra datos**: revisa que hayas completado el Paso 1 (Divipola y Departamentos) antes que cualquier otra fuente; todas las demás capas dependen de esas dos tablas.

**Una carga de archivo se queda "colgada" o excede el tiempo máximo**: el límite por defecto es 900 segundos; para archivos grandes puedes ampliarlo con la variable de entorno `OBSAN_PIPELINE_TIMEOUT_SECONDS` antes de iniciar la aplicación.

**Error relacionado con `fcntl` al ejecutar en Windows**: es un mensaje conocido del mecanismo de bloqueo de ejecuciones ETL; en Windows ese bloqueo queda desactivado automáticamente y no impide que el pipeline corra, pero sí significa que no debes lanzar dos pipelines a la vez manualmente en ese entorno.

## Referencia rápida de comandos

```bash
uv sync                                          # instalar dependencias
uv run streamlit run apps/streamlit/Geovisor.py  # levantar la app web
uv run -m src.runner "<carpeta_del_pipeline>"     # ejecutar un pipeline puntual
uv run -m src.scheduler                           # dejar corriendo el programador automático
pm2 start ecosystem.config.js                     # lo mismo, gestionado por PM2 (modo producción)
docker compose -f docker-compose.prod.yml up -d --build   # levantar todo con Docker
```
