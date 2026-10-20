# Operación y aceptación · plataforma técnica vigente

## Requisitos
Python 3.11, Docker + Docker Compose (solo integraciones). Los datos versionados en `data/sample/` son sintéticos. No copiar contraseñas a Git.

## Prueba base
```bash
python -m pip install -r requirements.txt
python -m pytest -q tests
python -m sunset.quality --input data/sample --scope full --report reports/quality_local.json
python -m sunset.offline_demo --input data/sample --report reports/offline_demo_verified.json
```

## Contenedores y perfiles
```bash
cp .env.example .env  # modificar secretos locales
docker compose config
docker compose --profile warehouse up -d postgres
docker compose --profile streaming up -d --build postgres kafka
docker compose exec kafka bash /work/streaming/init_topics.sh
docker compose --profile streaming up -d --build spark
```

Otros perfiles disponibles: `lake` (MinIO), `airflow`, `rag`, `monitoring`, `full`. Su puesta en marcha requiere recursos y redes compatibles. No usar `docker compose down -v` sobre datos reales.

## Evidencias actuales y condiciones
La [aceptación real #39357820940](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39357820940) aprobó los cinco jobs: PostgreSQL/Parquet/dbt, MinIO, Airflow, Kafka/Spark y RAG, más la compuerta agregada, usando datos sintéticos. La [ejecución real de SonarQube #39357820932](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39357820932) reportó 91.1% cobertura y Quality Gate OK. **Resultados correspondientes al commit evaluado entonces; deben renovarse si cambia el código o las pruebas.**

Las CI generan evidencia JSON e informes en `reports/` y los publican en *GitHub Actions artifacts*. La carpeta local `reports/` no está versionada.

## Restricciones explícitas
- Oracle 11g corporativo: pendiente servidor, acceso autorizado, librerías y esquema validado.
- Mantis: endpoint y contrato corporativo desconocidos.
- VPG, conversión y privacidad: requieren criterios de negocio, autorización y gobierno.
- Spark/Kafka tiene semántica *at-least-once*; replay mediante `event_id`, no exactly-once global.
- Publicación Parquet en disco local y transacciones de objetos S3 no son equivalentes.
- La prueba de 42.8M corresponde **solo** a PostgreSQL relacional, no al flujo completo.
- Airflow en `standalone` es demostrativo, no un despliegue de alta disponibilidad.
