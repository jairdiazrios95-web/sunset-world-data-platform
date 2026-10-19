# Verificación real de código y ejecución local — V0.4

**Fecha de la corrida:** 17 de octubre de 2026. **Fuentes:** exclusivamente sintéticas.

## Qué se ejecutó realmente

- Se extrajo `Sunset_World_Arquitectura_A_Repositorio_V03_Consolidado.zip` en una carpeta limpia.
- Se corrió `python -m pytest -q tests`: **31/31 PASS** en la base original.
- Se incorporó `sunset/verify_end_to_end_sqlite.py`: integra **10 fuentes** en tablas `raw_*` de un motor SQLite real, crea dimensiones de clientes y salas con versiones SCD2, dimensiones de fuente/producto/tiempo, tablas de hechos FX/tours/ventas/interacciones y efectúa conciliación de origen/destino.
- Se agregaron 3 pruebas automáticas para carga + reejecución, cambio histórico SCD2 y rechazo por pago huérfano; **35/35 PASS**.
- Se reingresaron las mismas filas por segunda ocasión. La huella SHA-256 del DW antes y después coincide; no se introdujeron duplicados ni alteraciones no deseadas.

### Ejecución mediana

| Fuente / control | Resultado ejecutado |
|---|---:|
| Leads | 20,000 |
| Pagos | 30,000 |
| Comentarios | 40,000 |
| Tours | 14,000 |
| Ventas | 2,800 |
| FX diario | 1,462 |
| Violaciones FK | **0** |
| Reejecución igual | **Sí** |
| Verificación SQLite (incluye calidad y dos cargas) | **3.013 segundos** |

### Ejecución ampliada

| Fuente / control | Resultado ejecutado |
|---|---:|
| Leads | **100,000** |
| Pagos | **150,000** |
| Comentarios | **200,000** |
| Clientes | 33,333 |
| Tours | 70,000 |
| Contratos | 14,000 |
| Ventas construidas en DW | 14,000 |
| Interacciones en DW | 200,000 |
| FX diario | 1,462 |
| Violaciones FK | **0** |
| Segundo replay altera el DW | **No** |
| Duración de la verificación SQLite | **17.458 segundos** |
| Duración de la generación CSV | 2.263 segundos |

El tiempo de verificación comprende calidad de datos, auditoría de relaciones, carga inicial, construcción del DW, segunda carga, reconciliación y cálculo de huellas. Estos son resultados locales de SQLite; **no son cifras de PostgreSQL, Oracle ni producción**.

Evidencias generadas:

- `reports/local_integration_verified.json` (muestra pequeña)
- `reports/benchmark_sqlite_verified.json` (ejecución mediana)
- `reports/large_sqlite_verified.json` (ejecución ampliada)
- `reports/local_pipeline_execution.json` (generación y ejecución de muestra)
- `reports/execution_environment_20261017.json` (capacidades del ambiente)
- `reports/offline_services_verified.json` (reglas de eventos y preparación de texto **sin vectores**)

## Cómo reproducir

Desde la raíz del repositorio:

```bash
python scripts/validar_local.py --leads 1000 --payments 600 --comments 1200
python scripts/validar_local.py --leads 100000 --payments 150000 --comments 200000 \
  --report reports/large_sqlite_verified.json
python -m pytest -q tests
```

**Nota:** el script usa solo bibliotecas estándar. `pytest` se necesita únicamente para la suite automática y viene incluido en el flujo de CI.

## Prueba local de reglas y texto (sin Kafka ni embeddings)

Se ejecutó también `python -m sunset.offline_services` sobre la muestra.
El script creó **5 eventos sintéticos** en SQLite con ID estable, detectó **2
alertas de ejemplo**, redactó textos y produjo **1,200 fragmentos de comentarios**
para una futura etapa de embeddings. La segunda ejecución no duplicó eventos ni
fragments. **No** usó un broker Kafka, Spark, modelo de embeddings, pgvector
ni la API Mantis. Es un control de reglas y preparación de textos, exclusivamente.

## Dependencias externas y límites comprobados

- `pyarrow`: no instalado en este runtime; intento de instalar versión fijada sin éxito porque no se puede resolver PyPI / no hay paquetes disponibles.
- `psycopg`: no instalado; no hay servidor PostgreSQL activo.
- `docker`: ejecutable ausente. Por tanto, no existe ejecución de contenedores Airflow, PostgreSQL, Kafka, Spark, MinIO o Grafana.
- `oracledb`: no instalado y no se recibió acceso a un Oracle 11g de Sunset World.
- `sentence-transformers` y `pgvector`: no disponibles aquí; sin consulta real a Mantis.
- Parquet: **no se escribió ningún `.parquet`**. Queda pendiente hasta un runtime con PyArrow. **No** se sustituyó por un archivo ficticio con extensión Parquet.

## Criterios pendientes antes de certificar la entrega integral

1. Validar metadatos Parquet (esquema, partición, tamaño, row groups y conteos) con PyArrow.
2. Ejecutar DDL real en PostgreSQL + extensión pgvector y comparar hechos/dimensiones con la demo SQL.
3. Completar DAG Airflow y `dbt run/test` contra ese PostgreSQL.
4. Ejecutar Kafka y Spark con eventos reales, checkpoints y sink `ON CONFLICT`.
5. Ejecutar embeddings con modelo cargado y consultas de recuperación en pgvector.
6. Ejecutar el perfil de 4.5 M + 6.9 M + 10 M de registros y producir benchmark de la arquitectura final. Ese perfil **no se considera probado**.

No se debe afirmar que 35 pruebas locales equivalen a un sistema productivo terminado. Este documento distingue evidencia real de diseño pendiente de ejecución.
