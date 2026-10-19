# SUNSET WORLD — Informe de integración V0.6

**Acción realizada:** revisión de compatibilidad de Oracle 11g, revisión de la estructura
Docker/CI, corrección del DDL de streaming, controles de integración real y gate de release.

## Evidencia ejecutada en este entorno

- Python: **51 tests PASS; 1 SKIPPED** (PyArrow no instalado).
- Calidad de CSV: **PASS (14 checks)**.
- Reconciliación con SQLite: **PASS (4 joins)**.
- Revisión de sintaxis Python y YAML: **PASS**.
- Prueba negativa de aceptación: **BLOCKED**, resultado correcto al faltar
  evidencia real.
- Librerías `pyarrow`, `psycopg`, `oracledb`, `pyspark`, `airflow`, `confluent_kafka`,
  `sentence_transformers`: **NO DISPONIBLES** aquí. Docker: **NO DISPONIBLE**.
- Se intentó `pip install` y `curl pypi.org`: **falló por falta de acceso al índice y DNS**.

## Integraciones físicas nuevas, preparadas NO ejecutadas

1. `live-platform.yml` ejecuta PyArrow→PostgreSQL/pgvector→dbt, reejecuta y concilia.
2. `live-minio.yml` sube Parquet a MinIO, lo descarga y valida SHA256 y reejecución.
3. `live-streaming.yml` manda eventos a Kafka, Spark procesa y valida PG y alertas.
4. `live-airflow.yml` ejecuta realmente la DAG con Airflow 2 y comprueba calidad.
5. `live-rag.yml` utiliza sentence-transformers real, pgvector y búsqueda semántica.

## Cambios correctivos

- Se eliminó el conflicto de tablas de alertas entre `003_upsert_patterns.sql`
  y `005_streaming.sql`.
- Se amplió el calendario de dimensiones a las fechas de tours y comentarios.
- Se añadió control de pgvector real (vector y distancia) y consulta RAG por modelo.
- Se dejó la configuración `requirements-oracle11g.txt` compatible con Thin/Thick:
  Oracle 11.2 requiere Thick y cliente OCI 19, además de credenciales reales.
- Se añadió un programa de aceptación que falla cerrado: `scripts/acceptance_gate.py`.

## Próxima condición técnica para liberar

Conectar GitHub, subir el contenido de este ZIP como repositorio y ejecutar Actions.
**Hasta observar los 5 workflows con PASS y artefactos reales, NO afirmar**
que PostgreSQL/MinIO/Airflow/Kafka/Spark/pgvector/RAG hayan pasado integración.
La conexión a Oracle productivo requiere autorización, schema y red privada.
La prueba anterior SQLite de 42.8 M no sustituye benchmark de motores reales.
