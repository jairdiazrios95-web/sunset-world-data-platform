# Guion ejecutivo y demostración — 5 minutos (grabación final pendiente)

**00:00–01:00 | Problema y propuesta.** Presentar el reto de datos fragmentados entre Voltux, Share CRM, TMS, Call Center y FX. Explicar por qué es necesario un Lake y una capa con hechos/dimensiones para analítica confiable.

**01:00–02:00 | Ingesta parametrizada.** Mostrar en terminal `config/pipeline.json`, el generador con perfiles `sample/volume`, la muestra generada y un `_manifest.json` de Parquet ya ejecutado en equipo compatible. Diferenciar volúmenes empresariales de referencia (4.5M Voltux, 6.9M pagos, 10M comentarios) de volúmenes **realmente procesados**. No usar capturas simuladas.

**02:00–03:00 | Calidad y relaciones.** Ejecutar `make demo`, abrir `reports/offline_demo_verified.json`: 1,000 leads, 140 ventas, 600 pagos, 1,200 comentarios, 1,462 cotizaciones de FX, cuatro joins conciliados en la muestra. Luego mostrar, cuando exista, la salida real de `warehouse.build_marts` y dbt.

**03:00–04:00 | Orquestación y streaming.** Mostrar la DAG Airflow con dos ramas paralelas, seguida por DW → dbt → calidad; reiniciar un evento Kafka y comprobar deduplicación idempotente en PostgreSQL. Si esos servicios no se ejecutan, mencionar la limitación y no presentarla como demo real.

**04:00–05:00 | Mantis, impacto y siguiente fase.** Mostrar chunks, hashes y búsqueda por pgvector cuando estén probados. Explicar impacto esperado en FX, Client Journey y gobierno de datos, y los riesgos: APIs no disponibles, SCD2, contratos de origen, privacidad, presupuesto e infraestructura. Cerrar resaltando que la evidencia reproducible es más valiosa que una promesa de *exactly-once* sin validación.

**Nota de integridad:** Las cinco diapositivas de esta entrega son borrador ejecutivo. El MP4 final debe producirse después de ejecutar Airflow, PostgreSQL, Kafka, Spark y pgvector, idealmente con capturas de pantalla reales y voz del candidato; el examen lo recomienda, pero es opcional.
