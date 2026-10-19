# Estado de entrega — 0.3

**Repositorio acumulativo**: esta versión incluye el trabajo de V0.1 y V0.2 y amplía el alcance, sin modificar los ZIP históricos conservados fuera de esta carpeta.

| Apartado PDF | Archivos principales | Comprobación |
|---|---|---|
| P1 20 pts Ingesta Oracle/Parquet | `oracle/*`, `sunset/parquet.py`, `sunset/minio.py` | Código y pruebas unitarias; Oracle/Parquet no ejecutado |
| P2 20 pts Estrella/SCD2/SQL | `warehouse/*`, `sql/001_init.sql`, `sql/004_marts.sql`, dbt | Cuatro joins verificados en SQLite; Postgres no ejecutado |
| P3 15 pts Airflow | `airflow/dags/etl_sunset.py`, `dbt/*` | Código sintácticamente válido, runtime no ejecutado |
| P4 15 pts Streaming | `streaming/*`, `sql/005_streaming.sql` | Reglas y esquemas unit tested; Kafka/Spark no ejecutado |
| P5 10 pts Calidad | `sunset/quality.py`, `sunset/audit.py`, `sunset/drift.py`, `monitoring` | 14 reglas aplicadas a CSV; Grafana sin Docker |
| P6 10 pts Mantis | `rag/*` y pgvector DDL | Chunking/IDs/redacción unit tested; modelo/DB/API no ejecutados |
| P7 5 pts Presentación | `deliverables/*.pptx/*.pdf`, `docs/VIDEO_SCRIPT.md` | 5 láminas exportadas; MP4 real pendiente |
| P8 5 pts Código limpio | `config`, `.env.example`, Dockerfiles, CI, `tests`, `README` | 31 pytest + quality/SQLite pass; Compose pendiente |

**Resultados verificados:** `reports/validation_summary.json`, `reports/pytest_verified.txt`, `reports/offline_demo_verified.json`, `reports/quality_local.json`. No benchmarks inventados. **Volúmenes PDF:** Voltux ~4.5M registros totales, TMS 6.9M pagos, 10M comentarios (objetivos de prueba, no ejecuciones). La generación también produce auxiliares, por lo que el total físico excedería 21.4M.

**Para la entrega definitiva:** completar pruebas en Docker, documentar cómputo y volumetría reales, grabar MP4 mostrando servicios ejecutados y publicar en GitHub. Fuentes corporativas no disponibles: Oracle real, FX aportado, API Mantis, contratos negocio VPG y reglas de frases prohibidas. El repositorio se puede subir a Git, pero no se puede afirmar su verificación productiva.
