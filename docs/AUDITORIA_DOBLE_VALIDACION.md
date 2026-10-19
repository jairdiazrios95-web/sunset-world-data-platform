# Auditoría defensiva (19-oct-2026)

Este cambio añade validación negativa explícita de CSV: columnas duplicadas,
filas truncadas/sobrantes, claves naturales en blanco y valores opcionales
vacíos. La falta de estas pruebas provocaba que un registro incompleto
pudiera llegar a PostgreSQL o que calidad fallara con excepción en lugar de
registrar una violación de contrato.

## Alcance de la doble validación
La automatización `.github/workflows/audit-two-passes.yml` verifica
**dos intentos independientes** de la compuerta maestra
`.github/workflows/real-acceptance.yml`, sobre exactamente el **mismo SHA**.
El segundo es un `rerun` API que reinicia los jobs de integración reales; no
es una simple repetición de la inspección de los iconos verdes. El informe de
ambos intentos se sube como artifact con ID de corrida e intento.

## Limitaciones, que deben quedar abiertas
- Oracle 11g: adaptador existente, sin integración corporate real y sin
  reemplazo automático del modo CSV del DAG.
- Mantis: el retrieval pgvector se prueba, pero no la API empresarial.
- 42.8M: benchmark físico PostgreSQL, **no** carga end-to-end de 42.8M
  por Kafka, Airflow, MinIO ni RAG.
- El test streaming CI cubre eventos/replay, no una prueba de millones de
  transcripciones ni caos de múltiples nodos.
- No se declara seguridad/cumplimiento productivo ni gestión real de secretos.
