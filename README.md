> **Calidad de código (20-oct-2026):** [SonarQube real #39353048027](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39353048027), SHA `c1daac5`: **91.1% de cobertura**, **0 bugs**, **0 vulnerabilidades reportadas** y **0 code smells**. Quality Gate de cobertura total ≥90%: **OK**. Se documentaron excepciones específicas a la comprobación AWS `ExpectedBucketOwner` para endpoints MinIO (`python:S7608`). La instancia SonarQube es temporal; sus artifacts conservan la evidencia. No implica autorización de producción.

> **CI verificada — 19 de octubre de 2026:** los cinco jobs de integración
> real aprobaron en la misma ejecución de GitHub Actions y la compuerta final
> reportó `PASS`: [ejecución #39332219848](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39332219848),
> commit `2d42784`. Corresponde a servicios reales con **datos sintéticos**;
> no implica certificación de producción.
>
> **Volumetría PostgreSQL — PASS real:** [ejecución #39334943627](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627) (rama `stress/pg-42m-20261019`, commit `0f6b568`). **42,800,000 filas sintéticas** cargadas y conciliadas físicamente (9 M leads, 13.8 M pagos, 20 M comentarios), en **436.102 segundos**, con 0 huérfanos y replay sin duplicados. Es una prueba relacional en PostgreSQL, no 42.8 M end-to-end en todos los motores.
> Oracle 11g real, API Mantis corporativa y criterios de VPG siguen pendientes
> de accesos y definiciones del negocio.
>
> [Checklist de entrega y limitaciones](docs/ENTREGA_FINAL_CHECKLIST.md) ·
> [Historial de evidencia GitHub](docs/GITHUB_ACTIONS_EJECUCION_V07.md)

# Sunset World | Plataforma de datos demostrativa

[![Python CI](https://img.shields.io/badge/pytest-pruebas%20locales-blue)](#evidencia-verificada) [![Origen](https://img.shields.io/badge/datos-100%25%20sint%C3%A9ticos-teal)](#fuentes-y-vol%C3%BAmenes)

**Solución propuesta al examen técnico Data Engineer de Sunset World**, documentada contra el PDF recibido. Desarrollada como una plataforma modular con Python, Parquet, PostgreSQL/pgvector, Airflow 2, dbt, Kafka y Spark Structured Streaming. **No es un repositorio oficial ni contiene datos corporativos.**

**Entrega actualizada (19-oct-2026):** [Aceptación real 5/5 en main](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39336589227) (SHA `cbe4c38`) y [estrés PostgreSQL físico 42.8M](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627) (SHA `0f6b568`, 436.102 s, 0 huérfanos). **No** es una carga end-to-end de 42.8M por todos los motores. La API corporativa Mantis/Oracle no está verificada.

**Documento principal:** [Respuestas al examen PDF](deliverables/Sunset_World_Respuestas_Examen.pdf) | [Presentación ejecutiva (5 diapositivas)](deliverables/Sunset_World_Presentacion_Ejecutiva.pptx) | [Guion de video](docs/VIDEO_SCRIPT.md)

## Arquitectura (opción A aprobada)

```mermaid
flowchart LR
  SRC[Oracle 11g / CSV sintéticos / FX] --> ING[Extractor parametrizado]
  ING --> LAKE[(Parquet / MinIO)]
  LAKE --> RAW[(PostgreSQL raw)]
  RAW --> DW[(Data Warehouse SCD2)]
  DW --> DBT[dbt / tests]
  DBT --> BI[FX / Client Journey / Finanzas]
  LAKE --> EMB[Chunking / embeddings]
  EMB --> VEC[(pgvector)]
  VEC --> RAG[Mantis - interfaz hipotética]
  CALL[Call Center] --> KAFKA[(Kafka)]
  KAFKA --> SPARK[Spark Streaming]
  SPARK --> RAW
  AIR[Airflow 02:00] -.orquesta.-> ING
  AIR -.orquesta.-> DBT
```

## Inicio rápido: ejecución que SÍ funciona sin Docker

Requiere Python 3.11+ con `pytest` instalado. El generador, la auditoría y la demostración SQLite solo usan la biblioteca estándar de Python.

```bash
python -m pytest -q tests
python -m sunset.cli audit --input data/sample
python -m sunset.quality --input data/sample --scope full --report reports/quality_local.json
python -m sunset.offline_demo --input data/sample --report reports/offline_demo_verified.json
# Ejemplo independiente y reproducible de generación:
python -m sunset.cli generate --profile sample --output data/generada
```

**Evidencia verificada en este entorno:** 35 tests en verde; 14 reglas principales de calidad ejecutadas; 1,000 leads, 1,462 FX, 600 pagos, 1,200 comentarios en la muestra; cuatro relaciones reconciliadas (ventas↔FX, contratos↔ventas, pagos↔contratos, comentarios↔tours); auditoría `PASS`. Ver reportes JSON en `reports/`. SQLite valida la lógica de joins, **no sustituye un servidor PostgreSQL**.

## Fuentes y volúmenes

| Volumen orientativo del PDF | Perfil `volume` (objetivo configurado) | Verificación real |
|---|---:|---|
| Voltux — **~4.5M registros en total del sistema** | 4,500,000 *leads_ventas* sintéticos, hipótesis para stress test | NO ejecutado a escala |
| TMS — ~6.9M pagos | 6,900,000 | NO ejecutado a escala |
| TMS — ~10M comentarios | 10,000,000 | NO ejecutado a escala |
| FX — diario | Fechas definidas en `generation` | 1,462 registros sintéticos probados |

**Aviso:** el perfil volumétrico también genera clientes, tours, contratos y dimensiones; por tanto, el **total físico generado será mayor que 21.4 millones de registros**. Los millones son referencias del examen, no un benchmark realizado ni un layout oficial.

La generación y procesamiento por lotes se controlan en [`config/pipeline.json`](config/pipeline.json), incluyendo semilla, fechas, perfiles, llaves, columnas obligatorias, tamaño de lote, compresión y máximo de filas por archivo. Sin secretos hardcodeados: usar `.env` local (no versionado).

## Validación real sin instalaciones en la computadora del evaluador (V0.4)

Se incorporó un **banco de integración SQL local**, totalmente ejecutable con
Python estándar y SQLite. Reproduce carga de diez tablas raw, dimensiones
SCD2, FX, hechos de ventas/tours/interacciones, conciliación de FK, segunda
carga idéntica y un control de rechazo de registros huérfanos.

```bash
python scripts/validar_local.py --leads 1000 --payments 600 --comments 1200
# Generar una prueba mayor, también sin dependencias extra:
python scripts/validar_local.py --leads 100000 --payments 150000 --comments 200000
python -m pytest -q tests   # cuando pytest esté instalado
```

**Evidencia ejecutada en este entorno (17-oct-2026):** 35/35 pruebas;
100,000 leads, 150,000 pagos y 200,000 comentarios más sus fuentes relacionadas;
14,000 ventas, 70,000 tours, 1,462 FX y 200,000 interacciones reconciliadas;
0 violaciones de FK; segunda ejecución idéntica según SHA-256; duración del
paso de verificación SQL local: 17.458 s (generación excluida).

Estos resultados son de **SQLite**, no resultados medidos de PostgreSQL.
Además se verificaron reglas y preparación de texto sin brokers ni vectores:

```bash
python -m sunset.offline_services --comments data/sample/comentarios.csv
```

Se observaron 5 eventos sintéticos, 2 alertas por reglas y 1,200 chunks,
con repetición idempotente (NO es una ejecución de Kafka ni de embeddings).

Este entorno no tiene Docker, PyArrow ni Psycopg y tampoco acceso DNS a PyPI;
la escritura real de Parquet, PostgreSQL, Airflow, Kafka, Spark y pgvector
permanece sin probar. No se deben extrapolar los tiempos a producción.
Ver [`docs/VALIDACION_REAL_SIN_DOCKER.md`](docs/VALIDACION_REAL_SIN_DOCKER.md).

## Fases de ejecución (requieren entorno con internet y Docker)

### 1. CSV -> Parquet BRONZE

```bash
python -m pip install -r requirements.txt
python -m sunset.parquet --input data/sample --output data/parquet --config config/pipeline.json
# Una fuente específica para paralelismo / Airflow:
python -m sunset.parquet --source leads_ventas --input data/sample --output data/parquet
```

La escritura limita memoria, particiona `year=AAAA/month=MM`, rota archivos por máximo de filas, compara filas y metadatos de Parquet, guarda `_manifest.json` y reemplaza snapshot con respaldo. En **objeto S3**, usar `sunset/minio.py` (manifest pointer) o diseñar commit transaccional Iceberg; un directorio POSIX no implementa transacciones S3.

### 2. PostgreSQL `raw` y modelo dimensional

```bash
cp .env.example .env      # definir secretos antes de iniciar servicios
# Docker local; primero verificar configuración y puertos:
docker compose config
docker compose --profile warehouse up -d postgres
# Exportar DSN de forma segura (NO escribir contraseñas en repositorio):
export SUNSET_PG_DSN='postgresql://USUARIO:CLAVE@localhost:5432/sunset'
for source in clientes leads_ventas tours contratos pagos comentarios fx_historico salas fuentes productos; do
  python -m warehouse.load_parquet --source "$source" --lake data/parquet
done
python -m warehouse.build_marts
```

DDL en `sql/001_init.sql`; `sql/003_upsert_patterns.sql` y `sql/005_streaming.sql` se cargan al crear por **primera vez** el volumen de PostgreSQL. Esquema dimensional y consultas en `sql/004_marts.sql` y `sql/002_business_queries.sql`. SCD tipo 2 para clientes/salas y vistas de KPIs **provisionales hasta validación de negocio**. Ejecute `dbt run/test` en su entorno o con Airflow.

### 3. Airflow / dbt

```bash
docker compose --profile airflow up -d --build airflow
# UI: http://localhost:8080; activar DAG etl_sunset
# Dependencias: leads y FX en paralelo -> fuentes restantes -> marts -> dbt -> calidad.
```

Airflow se fija a 2.10.5 porque el PDF solicita `sla`. Alertas webhook configurables por `ALERT_WEBHOOK_URL`; email solo cuando se configure servidor de correo. Es un contenedor `standalone` de demostración, **no producción HA**.

### 4. Kafka / Spark Structured Streaming

```bash
docker compose --profile streaming up -d --build kafka postgres
# Ejecutar dentro del contenedor Kafka:
docker compose exec kafka bash /work/streaming/init_topics.sh
# Productor desde terminal host tras instalar streaming/requirements.txt:
python -m streaming.producer --bootstrap localhost:9092
# Arrancar job Spark, revisar Maven packages, JDBC, logs y checkpoints:
docker compose --profile streaming up -d --build spark
```

Topics: `transcripciones_raw`, `transcripciones_procesadas`, `alertas`; key = `call_id`. **Garantía real de diseño:** entrega al menos una vez; PostgreSQL `ON CONFLICT(event_id)` da idempotencia lógica. No afirmar exactly-once end-to-end sin pruebas y contratos transaccionales.

### 5. Mantis / pgvector

```bash
python -m pip install -r requirements-ai.txt
export RAG_MODEL='sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'
python -m rag.build_embeddings --lake data/parquet
python -m rag.search '¿Qué opinan los clientes sobre el recorrido?'
```

Lectura de Parquet, limpieza limitada de información personal en fixtures, chunking parametrizado, SHA-256 para cambios, modelo multilingüe y vectores pgvector (384 dimensiones). Sin endpoint Mantis corporativo y sin autorización de PII. **No ejecutar sobre registros reales sin gobierno y seguridad aprobados.**

### 6. Observabilidad (opcional)

`docker compose --profile monitoring up -d grafana` levanta un tablero versionado en `monitoring/dashboards`. Abrir `localhost:3000` con el secreto `GRAFANA_ADMIN_PASSWORD` definido en `.env`. Requiere PostgreSQL disponible.

## Estructura del repositorio

```text
sunset/               generación, auditoría, calidad, Parquet, demo SQLite, drift, MinIO
warehouse/            landing Parquet->Postgres y construcción del DW
oracle/               adaptador read-only Oracle 11g Thick y extracción por ventanas
sql/                  DDL, esquema estrella, SCD2, UPSERT, streaming y KPIs
airflow/dags/         DAG TaskFlow diaria 02:00
streaming/            Kafka producer, reglas, Spark Structured Streaming
rag/                  embeddings incrementales y retrieval pgvector
dbt/                  staging, vistas de negocio, tests de SQL
notebooks/            notebook reproducible sin datos corporativos
config/               contratos JSON y perfiles sample/volume
reports/              evidencia ejecutada en demo Python/SQLite
deliverables/          respuestas PDF y diapositivas PPTX/PDF
docs/                  runbook, criterios, diagramas, guion de video y decisiones
tests/                 pruebas automatizadas (pytest)
.github/workflows/     CI Python
```

## Evidencia verificada / limitaciones

**Sí probados:** Python, banco de pruebas SQL SQLite del modelo dimensional, replay idempotente, generador con tamaño por bloque, repetibilidad, contratos, errores intencionales, calidad, integridad cruzada de muestra, SQL emulado para joins en SQLite (no DW). **No probados aquí:** dependencias binarias de Parquet, conexión Oracle, PostgreSQL, Docker Compose, Airflow, Spark, Kafka, dbt en BD real, pgvector ni la API Mantis. No se declara un entorno productivo ni un throughput medido de millones.

Antes de publicar un video final de cinco minutos, ejecutar en Docker y capturar salidas auténticas. [Runbook de aceptación](docs/OPERACION_Y_ACEPTACION.md) | [Preguntas y riesgos](docs/DECISIONS_PENDING.md) | [Respuestas completas](docs/RESPUESTAS_EXAMEN.md).

## Seguridad, versiones y supuestos

- Ningún servicio real de Sunset se consultó. Las fechas, IDs, salones, monedas y montos en CSV son sintéticos.
- No versionar `data/volume`, `data/parquet`, secretos ni pesos de modelos.
- Backfills, snapshots de origen, retención de PII y alteraciones de esquema requieren políticas de negocio y DBA.
- `VPG`, conversión, calidad en producción y embebidos Mantis requieren aprobación funcional; en el ejemplo se usan vistas `*_provisional`.
- Empaquetado **para subir a GitHub**, tras completar la batería de integración pendiente, no como certificación productiva.

## Prueba adicional de estrés — V0.5

Se incluyó un **simulador relacional SQL con lotes, FK, checkpoint, recuperación e idempotencia**.
Se ejecutaron físicamente 23.5 millones y 42.8 millones de registros principales sintéticos
más entidades y hechos. Los resultados se documentan en [Pruebas de estrés V0.5](docs/PRUEBAS_DE_ESTRES_V05.md).

Para empezar sin servicios externos: `make stress-smoke`. Para mayor volumen:
`make stress-plus` o `make stress-double` (requieren espacio y tiempo).
Esto **no certifica** ejecución en Oracle, Parquet, PostgreSQL, Kafka o Spark;
la integración real sigue pendiente de infraestructura compatible.

### Volumetría adicional verificada

Se generaron también los CSV originales del proyecto para el perfil + de
23.5 M de registros principales y su auditoría dio `PASS`. El perfil SQL
relacional 2x alcanzó 42.8 M con `PASS`. **41 pruebas Python aprobaron;
la integración PyArrow quedó omitida localmente** y tiene un workflow de CI
preparado pero todavía no ejecutado en GitHub.

Ver `docs/PRUEBAS_DE_ESTRES_V05.md` y reportes JSON para límites exactos.
