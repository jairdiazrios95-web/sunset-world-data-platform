# Sunset World — Informe técnico V0.7 (17-oct-2026)

## Alcance de la nueva iteración

Se conservó la arquitectura A y las funcionalidades V0.1–V0.6; se reforzó la preparación de **ejecución real** con GitHub Actions y una compuerta automática de aceptación que agrupa cinco tecnologías. Se añadió carga física configurable para PostgreSQL, con perfiles que alcanzan el doble de las cifras de referencia.

### Ejecutado realmente en el ambiente de desarrollo

- `python -m pytest -q tests`: **61 aprobadas, 1 omitida** (integración PyArrow porque no está instalado aquí).
- `python -m sunset.quality --input data/sample --scope full`: **PASS**, 14 controles, sin errores reportados.
- `python -m sunset.offline_demo --input data/sample`: **PASS**, 4 conciliaciones de joins; demuestra lógica local SQLite, no PostgreSQL.
- `python scripts/acceptance_gate.py ...`: **BLOCKED**, comportamiento correcto por ausencia de cinco evidencias externas auténticas.
- Todos los archivos `.github/workflows/*.yml`: inspeccionados sintácticamente con PyYAML; pruebas de contrato de GitHub Actions incluidas.
- Se creó historial Git local en `main`, sin credenciales ni remote configurado.

### Implementado, pero no ejecutado en la infraestructura real de GitHub

| Componente | Archivo | Estado real |
|---|---|---|
| Orquestación automática de CI integral | `.github/workflows/real-acceptance.yml` | No se ha publicado/disparado GitHub Actions |
| PostgreSQL/pgvector + PyArrow + dbt | `.github/workflows/live-platform.yml` | No ejecutado en runner |
| MinIO S3 + Parquet | `.github/workflows/live-minio.yml` | No ejecutado en runner |
| Apache Airflow + dbt + PostgreSQL | `.github/workflows/live-airflow.yml` | No ejecutado en runner |
| Apache Kafka + Spark Streaming | `.github/workflows/live-streaming.yml` | No ejecutado en runner |
| Embeddings de modelo real + pgvector | `.github/workflows/live-rag.yml` | No ejecutado en runner |
| Volumetría PostgreSQL física configurable | `.github/workflows/live-volume.yml` | No ejecutado en runner |
| Oracle 11g Thick | `oracle/` | Sin servidor y credenciales autorizadas |

**Criterio para declarar aprobación de GitHub Actions:** obtener el enlace de la corrida, SHA del commit, artifact `sunset-real-acceptance-summary` con `status: PASS`, y los cinco artifacts `sunset-proof-*` del mismo run. En su defecto indicar **pendiente**, incluso si los archivos existen en Git.

**Advertencia:** probar cargas en un simulador o en servicios efímeros no garantiza ausencia de errores de un entorno corporativo. El perfil `double` puede exceder capacidad del runner estándar y causar consumo adicional de minutos/recursos.
