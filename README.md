# OBSAN Geovisor — Guía de instalación y ejecución

Repositorio unificado del Observatorio de Seguridad Alimentaria y Nutricional (OBSAN): el proceso ETL que recolecta y prepara los datos, y el Geovisor, la aplicación web en Streamlit que los visualiza.

Esta guía cubre todo lo necesario para levantar el sistema desde cero, léela en orden: la sección "Cargar los datos" es la que realmente determina si el mapa se ve completo o vacío.

## Requisitos previos

- Python 3.11
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (gestor de entorno y dependencias del proyecto) — solo si vas a instalar **sin** Docker
- PostgreSQL 16 con la extensión PostGIS habilitada — solo si vas a instalar **sin** Docker (con Docker ya viene incluido)
- Docker y Docker Compose — solo si vas a instalar **con** Docker (local o en AWS)
- Git

## Estructura principal del repositorio

```text
Geovisor_OBSAN/
├── apps/
│   └── streamlit/
│       ├── Geovisor.py          ← página principal (mapa)
│       ├── pages/                ← hojas adicionales (carga de archivos, ejecuciones ETL, series temporales y correlaciones)
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
```

## Tres formas de desplegar el sistema

Elige una. Las tres terminan en el mismo lugar (la app corriendo en el puerto 8501), pero cambian cómo se ejecutan los comandos de la sección "Cargar los datos" más abajo — por eso es importante saber cuál elegiste antes de continuar.

| Camino | Cuándo usarlo | Dónde vive Postgres |
|---|---|---|
| **A — Docker Compose local** | Quieres todo funcionando en tu máquina sin instalar Postgres a mano | En un contenedor, en tu equipo |
| **B — AWS EC2 (Docker)** | Quieres que el Geovisor sea accesible desde internet, no solo desde tu máquina | En un contenedor, dentro de la instancia EC2 |
| **C — Entorno local sin Docker** | Estás desarrollando y prefieres tener Postgres y Python instalados directamente | En tu máquina, instalado aparte |

Los caminos A y B usan exactamente los mismos contenedores y el mismo `docker-compose.prod.yml` — la única diferencia es que B corre dentro de una máquina virtual de AWS en vez de tu computador. Por eso, de aquí en adelante, cualquier instrucción marcada **"Con Docker"** aplica igual para A y B.

### Camino A — Docker Compose local

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

### Camino B — AWS EC2 (Docker)

Es el equivalente en la nube del Camino A: en vez de correr Docker en tu propia máquina, lo corres en una máquina virtual dentro de AWS, accesible desde internet con la IP pública de la instancia.

**1. Lanzar la instancia EC2.** Desde la consola de AWS: EC2 → Instancias → Lanzar instancia, y configura:

- **Imagen (AMI)**: Ubuntu Server 22.04 LTS
- **Tipo de instancia**: al menos `t3.medium` (2 vCPU / 4 GB de RAM) — la base de datos, la app y el programador de ETL corren juntos en la misma máquina
- **Almacenamiento**: al menos 20 GB (gp3)
- **Par de claves (key pair)**: crea uno nuevo o usa uno existente, y descarga el archivo `.pem` — lo necesitas para conectarte por SSH

**2. Configurar el grupo de seguridad** (firewall de la instancia). Antes de lanzar, o editándolo después, agrega estas reglas de entrada:

| Tipo       | Puerto | Origen                                    | Para qué                                  |
|------------|--------|--------------------------------------------|--------------------------------------------|
| SSH        | 22     | Tu IP                                      | Conectarte a administrar la instancia       |
| Custom TCP | 8501   | 0.0.0.0/0 (o tu IP, si quieres restringirlo) | Acceder al Geovisor desde el navegador      |

No abras el puerto 5432 (Postgres) a internet — solo lo necesitan los contenedores entre sí, dentro de la misma máquina.

**3. Conectarte por SSH:**

```bash
chmod 400 mi-llave.pem
ssh -i mi-llave.pem ubuntu@<IP-PUBLICA-DE-LA-INSTANCIA>
```

**4. Instalar Docker en la instancia:**

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Opcional: evita tener que escribir "sudo" antes de cada comando docker
# (requiere cerrar la sesión SSH y volver a entrar para que tome efecto)
sudo usermod -aG docker $USER
```

**5. Clonar el repositorio y levantar el sistema** — igual que el Camino A, ejecutado dentro de la instancia:

```bash
git clone <url-del-repositorio>
cd Geovisor_OBSAN

cp .env.production.example .env.production
nano .env.production
# Cambia DB_PASSWORD / POSTGRES_PASSWORD por una clave propia, guarda con Ctrl+O y sal con Ctrl+X

docker compose -f docker-compose.prod.yml up -d --build
```

**6. Acceder al Geovisor:** abre en tu navegador `http://<IP-PUBLICA-DE-LA-INSTANCIA>:8501`. Los contenedores quedan corriendo en segundo plano (`-d`), así que puedes cerrar la sesión SSH sin que la aplicación se detenga.

Consideraciones adicionales de este camino:

- **IP fija**: la IP pública de una instancia EC2 cambia si la detienes y la vuelves a iniciar. Si necesitas una dirección estable, asigna una Elastic IP.
- **Persistencia de datos**: `docker-compose.prod.yml` guarda los datos de Postgres en un volumen dentro del disco de la instancia. Detener (`stop`) e iniciar (`start`) la instancia conserva los datos; terminar (`terminate`) la instancia los borra.
- **Costos**: una instancia `t3.medium` corriendo de forma continua no está cubierta por la capa gratuita de AWS.
- **Base de datos administrada (opcional)**: para algo más robusto que una sola instancia, la base de datos puede moverse a Amazon RDS para PostgreSQL (con PostGIS habilitado) en vez de vivir dentro del contenedor `db`; eso implica ajustar `docker-compose.prod.yml` para que `web` y `scheduler` apunten a ese endpoint externo.

### Camino C — Entorno local sin Docker

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

En ningún camino hace falta ejecutar migraciones aparte: cada pipeline crea sus propias tablas la primera vez que corre (`CREATE TABLE IF NOT EXISTS`), como parte de la carga.

## Cómo leer los comandos a partir de aquí

De aquí en adelante, cada vez que un pipeline se ejecuta por consola, el comando cambia según el camino que elegiste, porque en los caminos A y B el entorno de Python vive **dentro** del contenedor `web`, no en tu terminal:

- **Sin Docker (Camino C)**: ejecuta el comando tal como aparece, empieza con `uv run`.
- **Con Docker (Caminos A o B, incluido AWS)**: antepón `docker compose -f docker-compose.prod.yml exec web` antes de `uv run`.

Ejemplo del mismo comando en ambas variantes:

```bash
# Sin Docker
uv run -m src.runner "api_victimas"

# Con Docker (local o AWS)
docker compose -f docker-compose.prod.yml exec web uv run -m src.runner "api_victimas"
```

El resto de esta guía muestra ambas variantes explícitamente cuando aplica.

## Cargar los datos: cómo obtener un Geovisor completamente funcional

Esta es la parte que de verdad "llena" el mapa. Se hace en tres pasos, en este orden, y aplican igual sin importar el camino de instalación que hayas elegido (los tres se ejecutan por consola, con el prefijo de Docker cuando corresponda).

### Paso 1 — Referencia geográfica (obligatorio, antes que cualquier otra cosa)

Todas las demás capas se relacionan con el territorio a través de los códigos de municipio y departamento. Sin este paso, ninguna otra capa va a mostrar nada así se cargue correctamente.

Divipola, Departamentos y Subregiones son variables de uso interno (no aparecen en la página "Carga de Archivos" a propósito, porque casi nunca cambian) y solo se cargan por consola. Descarga los archivos parquet desde `https://zenodo.org/records/21633696` y cárgalos así:

```bash
# Sin Docker
OBSAN_INPUT_FILE=/ruta/al/archivo/divipola.parquet uv run -m src.runner "dw_divipola"
OBSAN_INPUT_FILE=/ruta/al/archivo/departamentos.parquet uv run -m src.runner "dw_departamento"
OBSAN_INPUT_FILE=/ruta/al/archivo/subregiones_nacional.parquet uv run -m src.runner "dw_subregiones_antioquia"

# Con Docker (local o AWS) — el archivo debe estar dentro del contenedor o accesible por una ruta montada
docker compose -f docker-compose.prod.yml exec -e OBSAN_INPUT_FILE=/ruta/al/archivo/divipola.parquet web uv run -m src.runner "dw_divipola"
docker compose -f docker-compose.prod.yml exec -e OBSAN_INPUT_FILE=/ruta/al/archivo/departamentos.parquet web uv run -m src.runner "dw_departamento"
docker compose -f docker-compose.prod.yml exec -e OBSAN_INPUT_FILE=/ruta/al/archivo/subregiones_nacional.parquet web uv run -m src.runner "dw_subregiones_antioquia"
```

Divipola también viene incluido en el repositorio como respaldo (`data/obligatory/divipola.parquet`), por si el enlace de Zenodo no está disponible.

Municipios PDET sigue el mismo patrón, con un archivo Excel descargado de la Agencia de Renovación del Territorio:

```bash
# Sin Docker
OBSAN_INPUT_FILE=/ruta/al/archivo/municipios_pdet.xlsx uv run -m src.runner "dw_mun_pdet"

# Con Docker (local o AWS)
docker compose -f docker-compose.prod.yml exec -e OBSAN_INPUT_FILE=/ruta/al/archivo/municipios_pdet.xlsx web uv run -m src.runner "dw_mun_pdet"
```

### Paso 2 — Fuentes automáticas (conectadas a una API pública)

Estas fuentes no requieren ningún archivo: se conectan solas a portales de datos abiertos.

**Con Docker (local o AWS)**: el `scheduler` ya es uno de los tres contenedores que se levantó en el paso anterior — no tienes que hacer nada para que corra. Para confirmar que está trayendo datos:

```bash
docker compose -f docker-compose.prod.yml logs -f scheduler
```

Si no quieres esperar al horario definido en `ecosystem.config.js`, dispara una fuente puntual:

```bash
docker compose -f docker-compose.prod.yml exec web uv run -m src.runner "api_victimas"
```

**Sin Docker (Camino C)**: tienes que dejar el programador corriendo tú mismo, en una terminal aparte:

```bash
uv run -m src.scheduler
# o, gestionado por PM2 (igual a como corre en producción):
pm2 start ecosystem.config.js
```

O disparar una fuente puntual sin esperar al horario:

```bash
uv run -m src.runner "api_victimas"
```

Fuentes automáticas disponibles: `api_edu_escolar`, `api_edu_superior`, `api_erradicacion_cultivos_coca`, `api_familias_accion`, `api_indice_riesgo_irca`, `api_minerales`, `api_oro_aluvion`, `api_produc_gas`, `api_produc_petroleo`, `api_regalias`, `api_victimas`, `url_terraclimate` (esta última tarda considerablemente más: descarga varios años de datos climáticos satelitales).

### Paso 3 — Fuentes manuales (vía la interfaz web)

Este paso es igual sin importar el camino de instalación, porque se hace desde el navegador, no por consola. Con la aplicación corriendo, entra a la hoja **Carga de Archivos** y sube, una por una, las variables que necesites. Cada formulario muestra el formato esperado y, cuando existe, un enlace de descarga directo a la fuente pública.

Variables disponibles en esta página: Población, Mercado laboral, Mortalidad por desnutrición, Desnutrición aguda en menores de 5 años, Bajo peso al nacer, Mercados campesinos, Censo pecuario (requiere indicar año y especie), Incidencia de Pobreza Multidimensional, Pobreza monetaria municipal, Coeficiente de GINI, Necesidades Básicas Insatisfechas (requiere indicar año), Producción Agrícola, Resguardo indígena.

La mayoría provienen de descargas públicas del DANE, SIVIGILA, ICA o UPRA; el formulario de cada variable indica la fuente exacta.

### Dato que no es de descarga pública

La capa central del observatorio, **Inseguridad alimentaria (Perfil Antioquia)**, depende de los resultados de una encuesta propia de la Universidad de Antioquia que no se descarga de un portal público. Sin ese archivo, específicamente esa capa quedará vacía aunque el resto del sistema funcione con normalidad; consíguelo con el equipo del observatorio y cárgalo desde la hoja de Carga de Archivos (variable "Perfil alimentario Antioquia").

## Verificar que quedó funcionando

- La aplicación carga sin errores de conexión a la base de datos (el ícono de estado en la barra lateral debe mostrarse en verde) — en `http://localhost:8501` (Caminos A y C) o `http://<IP-de-la-instancia>:8501` (Camino B).
- Al activar la capa "Municipios" o "Departamentos" en el mapa, se ven los polígonos del territorio (confirma que el Paso 1 quedó bien).
- Al activar alguna capa de la categoría Hidrocarburos o Educación, aparecen íconos o burbujas sobre el mapa (confirma que al menos una fuente automática cargó datos).
- La hoja "Ejecuciones ETL" muestra corridas registradas, no una lista vacía.

## Variables de entorno

| Variable       | Uso                                                              | Obligatoria |
|----------------|-------------------------------------------------------------------|-------------|
| `DB_HOST`      | Host de PostgreSQL (`db` con Docker, `localhost` sin Docker)     | Sí          |
| `DB_PORT`      | Puerto de PostgreSQL (por defecto 5432)                          | No          |
| `DB_NAME`      | Nombre de la base de datos                                        | Sí          |
| `DB_USER`      | Usuario de PostgreSQL                                             | Sí          |
| `DB_PASSWORD`  | Contraseña del usuario                                            | Sí          |
| `SOCRATA_APP_TOKEN` | Token opcional para las APIs de datos.gov.co (evita límites de tasa) | No |
| `OBSAN_INPUT_FILE`  | Ruta del archivo a procesar en pipelines de carga manual ejecutados por consola | Solo para esos pipelines |

## Solución de problemas comunes

**"Falta la variable de entorno obligatoria: DB_HOST" (u otra similar)**: falta el archivo `.env` (Camino C) o `.env.production` (Caminos A y B), o no tiene todas las variables de la tabla anterior.

**El mapa carga pero ninguna capa muestra datos**: revisa que hayas completado el Paso 1 (Divipola y Departamentos) antes que cualquier otra fuente; todas las demás capas dependen de esas dos tablas.

**Una carga de archivo se queda "colgada" o excede el tiempo máximo**: el límite por defecto es 900 segundos; para archivos grandes puedes ampliarlo con la variable de entorno `OBSAN_PIPELINE_TIMEOUT_SECONDS` antes de iniciar la aplicación.

**Error relacionado con `fcntl` al ejecutar en Windows**: aplica solo al Camino C (sin Docker) sobre Windows — es un mensaje conocido del mecanismo de bloqueo de ejecuciones ETL; queda desactivado automáticamente y no impide que el pipeline corra, pero sí significa que no debes lanzar dos pipelines a la vez manualmente en ese entorno. No aplica a los Caminos A/B, porque los contenedores corren Linux independientemente del sistema operativo anfitrión.

**Un comando `uv run ...` no funciona dentro de la instancia EC2 o en tu máquina con Docker**: revisa que le hayas puesto el prefijo `docker compose -f docker-compose.prod.yml exec web` — sin Docker instalado en el sistema operativo anfitrión, `uv` no existe fuera del contenedor.

## Referencia rápida de comandos

**Sin Docker (Camino C):**

```bash
uv sync                                          # instalar dependencias
uv run streamlit run apps/streamlit/Geovisor.py  # levantar la app web
uv run -m src.runner "<carpeta_del_pipeline>"     # ejecutar un pipeline puntual
uv run -m src.scheduler                           # dejar corriendo el programador automático
pm2 start ecosystem.config.js                     # lo mismo, gestionado por PM2
```

**Con Docker (Camino A local, o Camino B en AWS):**

```bash
docker compose -f docker-compose.prod.yml up -d --build              # levantar todo (db + web + scheduler)
docker compose -f docker-compose.prod.yml exec web uv run -m src.runner "<carpeta_del_pipeline>"  # pipeline puntual
docker compose -f docker-compose.prod.yml logs -f scheduler          # ver los logs del programador automático
docker compose -f docker-compose.prod.yml logs -f web                # ver los logs de la aplicación
docker compose -f docker-compose.prod.yml down                       # detener todo (conserva los datos)
```
