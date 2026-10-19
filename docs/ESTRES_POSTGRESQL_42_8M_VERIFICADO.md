# PostgreSQL 16 — resultado verificado de 42.8 millones (19-oct-2026)

## Evidencia
- [GitHub Actions run 39334943627](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627): **completed / success**.
- SHA: `0f6b568cd51262f6328224139d8741727d02e4af` (rama `stress/pg-42m-20261019`).
- Artifact: `sunset-postgres-physical-volume-double-39334943627`, que incluye `reports/live_pg_stress.json` y la muestra smoke.

| Tabla | Filas reales | Filas tras replay | Segundos |
|---|---:|---:|---:|
| leads | 9,000,000 | 9,000,000 | 56.184 |
| payments | 13,800,000 | 13,800,000 | 150.955 |
| comments | 20,000,000 | 20,000,000 | 218.855 |
| **Total** | **42,800,000** | **42,800,000** | **436.102 (total)** |

- Huérfanos en pagos y comentarios: **0**.
- Tamaño reportado por `pg_database_size`: **3,203,054,615 bytes**.
- Generación SQL `generate_series` en PostgreSQL real, transacciones por lotes de 100,000, `ON CONFLICT DO NOTHING`, FK y conciliaciones de conteo.

## Alcance
Esto comprueba carga **física relacional en PostgreSQL 16**, con datos sintéticos y verificación de replay. **No** es una prueba end-to-end de 42.8M filas atravesando Oracle 11g, Parquet/S3, Kafka/Spark, Airflow o RAG. Estos motores se validaron por separado con lotes sintéticos menores en [el run de integración 39332219848](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39332219848). No se afirma rendimiento de producción ni certificación de Oracle o API Mantis.
