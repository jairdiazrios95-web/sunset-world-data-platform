# Examen Sunset World: revisión de cumplimiento en diez controles (V0.3)

Se contrastaron los apartados y las solicitudes del PDF de 10 páginas con los archivos de este repositorio. No equivalen a diez ejecuciones de servicios: son **diez verificaciones de cobertura documental/técnica**.

| Control | Exigencia del PDF | Evidencia en repositorio | Estado |
|---|---|---|---|
| 1 | GitHub + ZIP + PDF + configuración + video recomendado | `README`, `.env.example`, Dockerfile, Compose, CI, `deliverables`, guion de video | PDF y deck listos; subir Git y grabar MP4 pendientes |
| 2 | Sistemas y volúmenes | `config/pipeline.json`, `sunset/generator.py`, 10 CSV | Demo 1,000 leads; escala configurada sin benchmark |
| 3 | Oracle→Lake incremental, Parquet, evolución esquema, reintentos | `oracle/snapshot_to_lake.py`, `sunset/parquet.py`, docs P1 | Código preparado; conexión/Parquet no ejecutados |
| 4 | Esquema estrella, SCD2, KPIs, identidad y fiscal | `sql/001_init.sql`, `sql/004_marts.sql`, `warehouse/build_marts.py` | Implementado; PostgreSQL no ejecutado |
| 5 | Airflow 02:00, paralelismo, dbt/calidad y alertas | `airflow/dags/etl_sunset.py`, `dbt`, docs P3 | DAG codificada; runtime pendiente |
| 6 | Kafka producer, 3 topics, Spark, checkpoints, SQL | `streaming/*`, `sql/005_streaming.sql` | Código; runtime Kafka/Spark pendiente |
| 7 | ≥5 pruebas de calidad, monitoreo, frescura, drift, linaje | `sunset/quality.py`, `sunset/drift.py`, `dbt/tests`, docs | 14 controles muestra; dashboard diseñado |
| 8 | Mantis embedding incremental desde Parquet/pgvector | `rag/build_embeddings.py`, `rag/search.py` | Código; modelo/DB/API no probado |
| 9 | Cinco diapositivas o video opcionales | PPTX/PDF 5 slides; `docs/VIDEO_SCRIPT.md` | Deck listo, video con captura real pendiente |
| 10 | Modularidad, variables, Docker, README, pruebas | `config`, Python, Docker, CI, `tests`, `reports` | 31 pruebas ejecutadas; infraestructura pendiente |

**Regla de liberación:** solo clasificar como `PASS` las pruebas presentes en `reports/validation_summary.json`; la infraestructura descrita queda como `NOT_EXECUTED` hasta completar la matriz del runbook.
