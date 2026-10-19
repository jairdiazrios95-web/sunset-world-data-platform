# Runbook de aceptación, seguridad y operación

## 1. Arranque local

1. `cp .env.example .env` y sustituir contraseñas de ejemplo por secretos generados. No versionar `.env`.
2. `python -m pip install -r requirements.txt` (entorno Python 3.11 con internet).
3. `make demo`: prueba unitaria + integridad/CSV + calidad local.
4. `python -m sunset.cli generate --profile sample --output data/sample` solo si deseas regenerar fixtures con semilla de configuración.
5. `python -m sunset.parquet --input data/sample --output data/parquet --config config/pipeline.json` y verificar `_manifest.json` para cada fuente.
6. `docker compose --profile warehouse up -d postgres` después de `docker compose config`.
7. `export SUNSET_PG_DSN=postgresql://<usuario>:<password>@localhost:5432/sunset`; para cada fuente (`clientes`, `leads_ventas`, `tours`, `contratos`, `pagos`, `comentarios`, `fx_historico`, `salas`, `fuentes`, `productos`) ejecutar `python -m warehouse.load_parquet --source FUENTE --lake data/parquet`.
8. `python -m warehouse.build_marts` y comprobar `status: PASS` y conteos reconciliados.
9. Instalar `dbt-postgres==1.9.0`, exportar `PGHOST`, `PGPASSWORD`, etc. y ejecutar `dbt run --project-dir dbt --profiles-dir dbt && dbt test --project-dir dbt --profiles-dir dbt`.
10. `docker compose --profile airflow up -d airflow`: en Airflow 2.10.x habilitar `etl_sunset` cuando el servidor PostgreSQL esté listo. Airflow standalone es un atajo de demo, no despliegue productivo.
11. Para Kafka: `docker compose --profile streaming up -d kafka`; crear topics con `docker compose exec kafka bash /work/streaming/init_topics.sh`, instalar dependencia Kafka y simular con `python -m streaming.producer`. Spark tiene una imagen propia con `psycopg`; antes de usarlo como prueba real, revisar `docker logs` y asegurar que el worker resuelve ambos.
12. Para RAG: instalar requirements de IA y ejecutar `python -m rag.build_embeddings --lake data/parquet`, luego `python -m rag.search 'Consulta de ejemplo'`.

## 2. Matriz de aprobación

| Gate | Condición verificable |
|---|---|
| Calidad de fuentes | `sunset.quality` PASS y no PK/FK inválidas |
| Parquet | Conteos CSV == suma metadata rowgroups == manifest; particiones ISO correctas |
| Reejecución | Mismo conteo de PK y contenido publicado sin duplicados |
| PostgreSQL | Tablas `raw` contienen exactamente las PK de snapshot; hechos=origen válido |
| Airflow | Dos fuentes core ejecutan en paralelo, downstream dbt/quality esperan ambas |
| Alertas | Inyectar error y comprobar webhook/email con secreto autorizado |
| Kafka | Orden estable por llamada a nivel partición, replays por event_id idempotentes |
| Streaming | Alerta esperada y evento normal almacenado sin duplicarse tras restart |
| RAG | Primer ciclo embebe; segundo ciclo con mismo hash genera 0 embeddings nuevos |
| Carga | Medir RSS, bytes, CPU, latencia y rows/s en hardware identificado |
| Seguridad | `.env` ausente de Git, credenciales no impresas, PII de demo únicamente |

## 3. Riesgos abiertos

- `pyarrow`/PostgreSQL/Docker/Kafka/Airflow/Spark/pgvector no probados en el entorno de creación: no declararlos productivos.
- Existen diferencias de compatibilidad entre Airflow 2, dependencias dbt, Java/Spark, imagen de Kafka y soporte de paquetes. Validar `docker compose config`, build e integración antes de entregar.
- Archivo de `/docker-entrypoint-initdb.d` de Postgres corre **solo al crear por primera vez** el volumen: para una base existente, ejecutar migraciones manuales o recrear un volumen de demo (con cuidado de datos).
- Modelo de embeddings escogido como propuesta de 384 dimensiones, sujetos a licencia, precisión en español y disponibilidad de descarga.
- Publicaciones Parquet con `rename` solo garantizan reemplazo local dentro de un filesystem; S3 necesita control de versión/manifest o Iceberg.
- Actualizar el watermark del consumidor únicamente después de su propio commit, no al extraer del origen.
- No incluir grandes archivos de carga en Git; usar perfiles y manifestos.
- La disponibilidad real de Mantis es desconocida; no inventar URL/API.
