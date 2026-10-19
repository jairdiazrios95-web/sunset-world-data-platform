# Guion de demostración de cinco minutos - Sunset World

**Fecha:** 19-oct-2026. **Estado:** guion, no video MP4 grabado. Mostrar únicamente evidencias reales de GitHub Actions.

## 00:00-01:00. Problema y solución
Explicar integración de Voltux, Share CRM, TMS, FX y Call Center mediante ingesta incremental, Data Lake Parquet/MinIO, PostgreSQL dimensional y dbt, orquestados con Airflow. Kafka/Spark atiende eventos y pgvector sirve como base para una futura integración autorizada con Mantis.

## 01:00-02:00. Motores funcionando
Mostrar [GitHub Actions #39336589227](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39336589227) (SHA `cbe4c38`): cinco jobs reales y compuerta final **SUCCESS**. Explicar los artifacts, no solo los iconos verdes.

## 02:00-03:00. Benchmark físico
Mostrar [estrés PostgreSQL #39334943627](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627), SHA `0f6b568`: 9M leads, 13.8M pagos y 20M comentarios = **42.8M filas** físicamente en PostgreSQL 16, **436.102 s**, cero huérfanos y conteos estables tras reenvío. Aclarar que **no** equivale a 42.8M end-to-end en todos los motores.

## 03:00-04:00. Calidad, idempotencia y RAG
Explicar manifest y reconciliación Parquet/MinIO, tareas DAG y dbt, Kafka/Spark con claves de evento, alertas en PostgreSQL, e indexación RAG con pgvector. El replay es idempotente en la base, pero no se promete exactly-once físico en todo Kafka.

## 04:00-05:00. Valor y limitaciones
Enfatizar FX, customer journey, conversión y gobierno. Oracle 11g corporativo, API Mantis, VPG, costos y PII siguen pendientes de acceso/definición. Cerrar con los dos enlaces de runs y criterios para producción.

**Nota:** capturar la pantalla auténtica y narración propia; esta documentación no incluye un MP4.
