# Entrega técnica Sunset World — verificación y límites (19-oct-2026)

## Evidencia ejecutada (no simulada)

- [x] [CI de aceptación 39332219848](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39332219848) — SHA `2d42784bea44ab3ead74d71cfb275401fb762d08`. Jobs PostgreSQL/Parquet/dbt/pgvector, MinIO, Kafka/Spark, Airflow, RAG y aceptación **SUCCESS**. Siete artifacts descargables de la misma ejecución. Datos de prueba sintéticos.
- [x] Integración PyArrow↔PostgreSQL y repetición sin duplicados, eventos Kafka↔Spark↔PostgreSQL, Airflow DAG real, modelo de embeddings↔pgvector, y subida/descarga MinIO comprobados en CI.
- [x] [PostgreSQL físico con 42.8 millones](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627), SHA `0f6b568`: **PASS verificado** (9M leads, 13.8M pagos, 20M comentarios; 0 huérfanos, replay estable, 436.102 s, 3,203,054,615 bytes de base). Es una prueba relacional, **no 42.8M end-to-end**.

## Restricciones explicadas con precisión

- Oracle 11g Thick: sin servidor, VPN, credenciales y cliente Oracle autorizado. No se afirma conexión real.
- API Mantis corporativa, reglas de negocio definitivas (VPG y denominador de conversión), sizing productivo y políticas de seguridad: deben ser confirmados por Sunset World.
- Ninguna ejecución sintética garantiza rendimiento/corrección productiva. Las credenciales de CI son efímeras y locales al runner.

## Revisión previa al correo

- [x] Confirmados conteos, bytes PostgreSQL y resultado del benchmark 42.8M en la ejecución #39334943627; artifact conservado en GitHub.
- [x] PR de hardening integrado y CI final aprobado en `main` (SHA `cbe4c38`, Actions #39336589227).
- [ ] Revisar PDF `deliverables/Sunset_World_Respuestas_Examen.pdf` y `deliverables/Sunset_World_Presentacion_Ejecutiva.pptx` frente al SHA final: pueden contener textos anteriores a las pruebas de GitHub. No presentarlos como actualizados hasta regenerarlos y verificar.
- [ ] Preparar y comprobar el enlace de repositorio privado para evaluadores autorizados. **No** abrir el repo público ni compartir tokens.
- [x] Opción comunicativa: presentación de cinco diapositivas y guion; no se declara video MP4.
- [ ] Redactar correo con evidencia accesible, alcance exacto, limitaciones y destinatarios confirmados.

## Procedimiento de evidencia

El informe debe citar run ID, commit SHA, URL, fecha, datos sintéticos y artifacts descargados; copiar los conteos exactos **solo** desde el JSON validado. En caso de falla de cualquier prueba requerida se conserva el indicador `BLOCKED`, y nunca se altera artificialmente la compuerta.
