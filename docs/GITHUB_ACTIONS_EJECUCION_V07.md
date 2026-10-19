# V0.7 — ejecución verificable con GitHub Actions

**Actualización verificada 19-oct-2026:** la ejecución [39332219848](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39332219848), commit `2d42784`, finalizó `success`. Los cinco jobs (platform, minio, airflow, streaming, rag) y la compuerta `acceptance` terminaron `success`. La compuerta imprimió `status: PASS`, y produjo siete artifacts (`sunset-proof-*`, `sunset-dbt-diagnostics`, `sunset-real-acceptance-summary`). **Alcance:** pruebas sintéticas sobre servicios reales; no hay acceso corporativo a Oracle ni autorización de producción.

**Stress físico — PASS verificado:** [run 39334943627](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627), commit `0f6b568`, artifact `sunset-postgres-physical-volume-double-39334943627`: 9,000,000 leads + 13,800,000 pagos + 20,000,000 comentarios = 42,800,000 filas físicas, 0 huérfanos, conteos estables tras replay, 3,203,054,615 bytes de base y 436.102 s de ejecución. **No** representa 42.8 M atravesando Spark, Oracle y MinIO.

## Circuito automático

`real-acceptance.yml` se dispara al primer `push` a `main` y también manualmente. Llama a los cinco workflows reutilizables de un mismo commit:

1. `live-platform.yml`: PostgreSQL 16 + pgvector + PyArrow + dbt; 10 fuentes, dos cargas e integridad.
2. `live-minio.yml`: MinIO con ficheros Parquet reales, checksum de descarga y replay.
3. `live-airflow.yml`: DAG Airflow 2 + dbt + PostgreSQL reales y comprobación independiente de tareas, fuentes y hechos.
4. `live-streaming.yml`: Kafka 3.9 + Spark 3.5 + PostgreSQL; eventos y alertas físicas con reenvío.
5. `live-rag.yml`: descarga y ejecuta un modelo multilingüe de sentence-transformers, pgvector y búsqueda.

El job `acceptance` solo puede terminar en verde cuando los cinco subjobs publican artifacts JSON de **esa misma ejecución**, con campos de evidencia coherentes. Si un proveedor, driver, repositorio Maven, container o prueba falla, el veredicto será `BLOCKED` / fallo, no una falsa aprobación. Las pruebas de la muestra no certifican la volumetría masiva en todos los motores.

GitHub Actions admite reutilización de workflows con `workflow_call`, contenedores de servicio en runners Ubuntu y artifacts dentro de la misma ejecución. Documentación: https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows y https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax

## Carga masiva sobre PostgreSQL real

El workflow opcional `live-volume.yml` se ejecuta desde **Actions → sunset-real-postgresql-volume-stress → Run workflow**. Tiene tres perfiles:

| Perfil | Leads | Pagos | Comentarios |
|---|---:|---:|---:|
| `smoke` | 10.000 | 15.000 | 20.000 |
| `reference` | 4.500.000 | 6.900.000 | 10.000.000 |
| `double` | 9.000.000 | 13.800.000 | 20.000.000 |

El generador usa PostgreSQL **físico**, `generate_series` por lotes, FK, checkpoints por transacción, `ON CONFLICT` y recarga del último bloque. La prueba `double` exige bastante espacio de disco, memoria y tiempo; un runner GitHub estándar podría agotar recursos. Si falla por ese motivo el resultado es **no aprobado** y habrá que correrla en un runner con mayor capacidad, no extrapolar desde SQLite. Este test valida almacenamiento/relaciones PostgreSQL, **no los 42.8 M recorriendo Oracle→Spark→S3 end-to-end**.

## Dependencia no sustituible: Oracle 11g real

La adaptación del código está preparada para `python-oracledb` **Thick**, pero no se dispone de un Oracle 11g autorizado ni su driver cliente de 19c en este ambiente. GitHub Actions tampoco puede inventar conectividad corporativa, VPN, credenciales o datos privados. Solo se puede cerrar esa parte mediante acceso autorizado al sistema o un entorno Oracle equivalente licenciado.

## Paso operativo de publicación

Publicar **archivos descomprimidos** del repositorio en `main`, no el ZIP como archivo único. Los workflow Actions estarán en `.github/workflows`; el `push` iniciará el flujo automático. Las credenciales que figuran en CI son **solo de contenedores efímeros**, nunca usar en sistemas empresariales. El código no requiere instalaciones en la PC del evaluador. En un repositorio privado, los minutos de GitHub Actions y las máquinas de mayor tamaño pueden consumir cuota o causar cargos, según el plan.

**Contrato de aprobación:** `reports/github_real_acceptance.json` y los cinco artifacts `sunset-proof-*` deben provenir de un **run real identificado**. Adjuntar link de ejecución, commit SHA, fecha y conteos; excluir cualquier declaración de garantía 100% de producción.
