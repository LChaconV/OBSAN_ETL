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

postgresql:
  user: ""       # Usuario de PostgreSQL."
  pass: ""       # Contraseña del usuario de PostgreSQL
  host: ""       # Dirección del servidor PostgreSQL."
  port:          # Puerto de PostgreSQL. 
  database: ""   # Nombre de la base de datos del proyecto. 

# Parámetros opcionales de rendimiento
pooling:
  min_size: 1
  max_size: 5
