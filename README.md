# Airflow Pipelines

Este repositorio está destinado a **varios pipelines de datos automatizados** utilizando Apache Airflow.  
Cada pipeline (DAG) tiene un propósito específico y puede incluir procesos ETL, integración con bases de datos, envío de notificaciones, entre otros.

---

##  Primer DAG: Carga diaria de ventas a MySQL

El primer DAG del proyecto se llama `mover_archivos_excel_sql_ventas` y realiza lo siguiente:

- **Carga diaria de archivos de ventas** desde la carpeta de origen hacia una base de datos MySQL.  
- **Manejo de errores**: si ocurre un fallo en la tarea, se envía un correo electrónico de notificación al responsable.  
- **Configuración segura**: las credenciales de la base de datos y del correo se almacenan en un archivo `.env` que **no se sube a GitHub**.

### Estructura de archivos relacionada:
├── dags/
│ └── etl_ventas.py # DAG principal
├── .env # Variables de entorno (no esta cargado en el repo)
├── docker-compose.yml # Configuración de Airflow y servicios
└── README.md

## Configuración

1. Copiar `.env.example` a `.env` y completar con tus credenciales:

```bash
cp .env.example .env
