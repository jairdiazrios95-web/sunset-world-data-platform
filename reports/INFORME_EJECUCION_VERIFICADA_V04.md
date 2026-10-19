# Informe de ejecución verificada — Sunset World V0.4

**Fecha:** 17 octubre 2026. **Datos:** 100% sintéticos. **Herramienta:** Python 3.13 + SQLite nativo.

## Resultados reales

| Evidencia | Resultado |
|---|---|
| Suite de pruebas | **35/35 PASS** |
| Muestra principal | 1,000 leads, 600 pagos, 1,200 comentarios |
| Prueba ampliada | 100,000 leads, 150,000 pagos, 200,000 comentarios |
| DW de prueba: tours / ventas / interacciones | **70,000 / 14,000 / 200,000** |
| Violaciones de FK (ampliada) | **0** |
| Reejecución idéntica (ampliada) | **True** |
| Tiempo ejecución ampliada en SQLite | **17.458 s** (+ generación CSV) |
| Eventos sintéticos probados | **5** |
| Alertas detectadas por reglas | **2** |
| Chunks preparados sin embedding | **1,200** |

## Lo que NO se puede declarar aprobado

PyArrow/Parquet físico, PostgreSQL real, Oracle 11g, Docker, Airflow, dbt real,
Kafka, Spark Streaming, pgvector/embeddings y Mantis. Este entorno carece de sus
runtimes o acceso a servicios. **No** se procesaron los 21.4 millones en
Parquet/PostgreSQL. No extrapolar velocidades de SQLite a producción.

## Ejecutar por cuenta propia

`python scripts/validar_local.py --leads 1000 --payments 600 --comments 1200`

`python -m sunset.offline_services --comments data/sample/comentarios.csv`

Consulte `docs/VALIDACION_REAL_SIN_DOCKER.md` y los archivos JSON de `reports/`.
