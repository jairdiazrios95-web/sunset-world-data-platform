# Entrega técnica Sunset World — verificación y límites (19-oct-2026)

## Evidencia ejecutada (no simulada)

- [x] [CI de aceptación 39332219848](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39332219848) — SHA `2d42784bea44ab3ead74d71cfb275401fb762d08`. Jobs PostgreSQL/Parquet/dbt/pgvector, MinIO, Kafka/Spark, Airflow, RAG y aceptación **SUCCESS**. Siete artifacts descargables de la misma ejecución. Datos de prueba sintéticos.
- [x] Integración PyArrow↔PostgreSQL y repetición sin duplicados, eventos Kafka↔Spark↔PostgreSQL, Airflow DAG real, modelo de embeddings↔pgvector, y subida/descarga MinIO comprobados en CI.
- [ ] [PostgreSQL físico con 42.8 millones](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627) — se inició ejecución real, **pendiente de verificar el resultado**. Es una prueba exclusivamente de capacidad PostgreSQL/FK/idempotencia, **no 42.8M end-to-end en todos los motores**.

## Restricciones explicadas con precisión

- Oracle 11g Thick: sin servidor, VPN, credenciales y cliente Oracle autorizado. No se afirma conexión real.
- API Mantis corporativa, reglas de negocio definitivas (VPG y denominador de conversión), sizing productivo y políticas de seguridad: deben ser confirmados por Sunset World.
- Ninguna ejecución sintética garantiza rendimiento/corrección productiva. Las credenciales de CI son efímeras y locales al runner.

## Revisión previa al correo

- [ ] Confirmar resultado, tiempos, uso de disco y conteos reales del benchmark 42.8M; si agota recursos, registrar BLOCKED y el límite comprobado, sin inventar PASS.
- [ ] Validar PR de hardening y ejecutar CI en el SHA definitivo de `main`.
- [ ] Revisar PDF `deliverables/Sunset_World_Respuestas_Examen.pdf` y `deliverables/Sunset_World_Presentacion_Ejecutiva.pptx` frente al SHA final: pueden contener textos anteriores a las pruebas de GitHub. No presentarlos como actualizados hasta regenerarlos y verificar.
- [ ] Preparar y comprobar el enlace de repositorio privado para evaluadores autorizados. **No** abrir el repo público ni compartir tokens.
- [ ] Determinar si la opción comunicativa exige diapositivas **o** video; el guion existe, pero el MP4 real no se ha verificado.
- [ ] Redactar correo con evidencia accesible, alcance exacto, limitaciones y destinatarios confirmados.

## Procedimiento de evidencia

El informe debe citar run ID, commit SHA, URL, fecha, datos sintéticos y artifacts descargados; copiar los conteos exactos **solo** desde el JSON validado. En caso de falla de cualquier prueba requerida se conserva el indicador `BLOCKED`, y nunca se altera artificialmente la compuerta.
