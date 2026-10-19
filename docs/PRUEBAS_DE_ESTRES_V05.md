# Prueba de estrés V0.5 — Sunset World (evidencias y alcance)

## Qué representa

El examen indica como **referencias aproximadas**: ~4.5 M de registros en Voltux (no necesariamente todos leads), ~6.9 M pagos TMS, ~10 M comentarios TMS. Para probar un límite más alto, se usaron dos perfiles sintéticos. **No son datos productivos de Sunset World.**

| Fuente | Referencia examen | Perfil + | Perfil 2x |
|---|---:|---:|---:|
| Voltux representado como leads | 4,500,000 | 5,000,000 | 9,000,000 |
| Pagos | 6,900,000 | 7,500,000 | 13,800,000 |
| Comentarios | 10,000,000 | 11,000,000 | 20,000,000 |
| TOTAL fuentes principales | 21,400,000 | 23,500,000 | 42,800,000 |

La prueba **física** se ejecutó sobre SQLite 3.46.1 en este contenedor Linux, con memoria cgroup 4 GiB, CPU compartida y lotes de 100 mil filas. Además de las fuentes anteriores, genera tablas de clientes, tours, contratos y tipos de cambio y materializa tres hechos SQL.

## Resultados observados (SQL relacional PROXY)

| Métrica | Perfil + | Perfil 2x |
|---|---:|---:|
| Resultado | PASS | PASS |
| Tiempo global, segundos | 47.506 | 93.289 |
| Pico RSS del proceso, MiB | 129.05 | 129.26 |
| Fact ventas | 700,000 | 1,260,000 |
| Fact pagos | 7,500,000 | 13,800,000 |
| Fact interacciones | 11,000,000 | 20,000,000 |
| Huérfanos contratos→leads, pagos→contratos, comentarios→tours y ventas→FX | 0 | 0 |
| Pruebas negativas de FK | Rechazadas correctamente | Rechazadas correctamente |
| Reejecución de hechos | Mismos conteos | Mismos conteos |

Fuente: `reports/stress_sqlite_plus_verified.json` y `reports/stress_sqlite_double_verified.json`, derivados de ejecuciones reales; contienen tiempo y parámetros completos. El máximo RSS es del **proceso Python**, no consumo total del host/caché de páginas. La velocidad de SQLite no puede extrapolarse a Oracle/PostgreSQL ni a una red.

## Cómo reproducirlo

Desde la raíz del repo (Python 3.11+; sin bibliotecas externas):

```bash
python -m pytest -q tests
python stress/sqlite_load.py --config config/stress_smoke.json --db data/stress/smoke.sqlite --report reports/stress_smoke.json
python stress/sqlite_load.py --config config/stress.json --db data/stress/plus.sqlite --report reports/stress_plus.json
python stress/sqlite_load.py --config config/stress_2x.json --db data/stress/double.sqlite --report reports/stress_double.json
```

**Inyección de fallo y recuperación:**

```bash
python stress/sqlite_load.py --config config/stress_smoke.json --db data/stress/recovery.sqlite --interrupt-after 4
# Exit 75 esperado: interrupción intencionada después del commit del cuarto lote.
python stress/sqlite_load.py --config config/stress_smoke.json --db data/stress/recovery.sqlite --report reports/recovered.json
```

El checkpoint es transaccional; el proceso bloquea parámetros distintos cuando se intenta reanudar una base creada con otra configuración. La prueba reproduce interrupciones entre lotes, NO corrupción física del disco ni fallas reales de red.

**Generador original de CSV:**

```bash
python -m stress.run_original_csv_generation --config config/stress.json --out data/stress/csv --report reports/stress_csv.json
python -m sunset.audit --input data/stress/csv --output reports/stress_csv_audit.json
```

La auditoría `sunset.audit` verifica claves, relaciones, fechas, duplicados e integridad de cruces. Los CSV completos y bases SQLite no se suben a Git por peso: se pueden regenerar con semilla reproducible.

### Cruces semánticos adicionales

Verificados sobre ambos perfiles SQL mediante
`stress/check_semantic_consistency.py`: consistencia cliente-contrato,
cliente-comentario, importe convertido con FX, ventas positivas y pagos
positivos. **0 discrepancias** en ambos perfiles.
Evidencia: `reports/stress_semantic_plus_verified.json`,
`reports/stress_semantic_double_verified.json`.

## Generación CSV real del proyecto y auditoría de 23.5 millones

El generador `sunset.generator.build` generó **10 archivos CSV** con 5,000,000 leads,
7,500,000 pagos, 11,000,000 comentarios y las correspondientes tablas de relaciones;
la ejecución tomó **123.77 s**, pico RSS del proceso **197.24 MiB**.

Después, se ejecutó la auditoría completa `sunset.audit` sobre esos archivos físicos:
- Estado: **PASS**
- **0** duplicados en llaves naturales de las diez fuentes.
- **0** huérfanos en once relaciones entidad-entidad y la relación ventas-FX.
- **0** errores de cliente consistente en tours/contratos.
- **0** fechas inválidas.

Archivos verificables: `reports/stress_csv_generation_verified.json`,
`reports/stress_csv_audit_verified.json`. Los CSV originales pesan ~2.2 GiB y se
omiten del repositorio; **su generación no equivale a la escritura Parquet**.

## Pruebas automatizadas

**41 aprobadas, 1 omitida** en este entorno (la integración PyArrow se omite por
no tener el paquete instalado). Se añadió `.github/workflows/parquet-integration.yml`
para ejecutar una conversión real CSV→Parquet y repetir la carga en un runner
que pueda instalar PyArrow; **ese workflow aún no se ha ejecutado en GitHub**.

## Limitaciones explícitas (muy importantes)

- **SQLite es un simulador relacional, no un emulador de PostgreSQL, Oracle, Kafka ni Spark.** Aquí se ejercitaron SQL, materialización, llaves, validaciones, reintentos y presión de volumen sintético. El generador SQL del simulador no utiliza el adaptador Oracle ni la escritura Parquet del repositorio.
- La carga utiliza un esquema reducido, distribución sintética regular y poco texto real. No reproduce la longitud completa de transcripciones, la cardinalidad/escasez de datos productivos, los sesgos, las llamadas REST, red, locking, latencia, paralelismo ni saturación de almacenamiento distribuido.
- **No se ha validado** ejecución física de Parquet/MinIO, PostgreSQL/dbt, Airflow, Spark/Kafka ni pgvector. Tampoco se ha probado el desempeño de modelos de embeddings. Un PASS en estos perfiles no garantiza que esas tecnologías no fallen.
- Para demostrar el examen con rigor, faltan pruebas reales en un ambiente compatible: ingestión Oracle 11g (o mock controlado), Parquet con PyArrow/MinIO, Docker Compose/PostgreSQL, DAG de Airflow, streaming Kafka→Spark y RAG con pgvector. Registrar sus logs y tiempos en `reports/` **solo después de ejecutarlos**.
- No llamar al tiempo SQLite «throughput Spark», «rendimiento PostgreSQL» ni «benchmark Oracle». El criterio objetivo es distinguir capacidad demostrada de hipótesis aún no comprobadas.

## Referencias técnicas de herramientas adicionales

DuckDB (Python) es una opción recomendada para una segunda etapa de analítica local y validación Parquet cuando esté instalado: https://duckdb.org/docs/stable/clients/python/overview. Su comportamiento no equivale al de PostgreSQL ni de Spark Streaming.

Spark Structured Streaming y `foreachBatch` requieren diseñar idempotencia en la escritura externa: https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html.
