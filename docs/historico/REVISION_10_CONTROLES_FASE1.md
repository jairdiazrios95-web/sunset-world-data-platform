# Sunset World — revisión del examen, 10 controles independientes

**Fuente primaria:** `Examen Data Engineer.pdf` (10 páginas), revisado frente a `Sunset_World_Fase1_Auditada.zip`.
**Fecha de revisión:** 16 de octubre de 2026. **Estado:** revisión documental y de estructura, **no** certificación de ejecución completa.

## 01 — Entrega y destinatarios (página 1)
GitHub público o privado (en caso privado, dar acceso a `atrejo@sunset.mx`); ZIP; código Python, SQL, Airflow DAG y notebooks; README con arquitectura/decisiones; PDF de respuestas abiertas; Dockerfile, docker-compose, requirements.txt, .env.example; video de cinco minutos opcional pero recomendado. **Fase 1:** no incluye todavía GitHub, Docker/Compose, notebook, PDF de respuestas ni video.

## 02 — Sistemas y volumetría (páginas 2-3)
Voltux Oracle 11g ~4.5 millones de registros (no necesariamente 4.5 millones de *leads*); Share CRM clientes y contratos; TMS ~10 millones de comentarios y 6.9 millones de pagos; call center audio/texto; finanzas SAP/Oracle; FX diario. La muestra sintética ya existe; **no** se han ejecutado las volumetrías completas. Los esquemas CSV asumidos no son esquemas oficiales. No hay CSV originales adjuntos a esta entrega, pese a que el examen habla de `leads_ventas.csv` y `fx_historico.csv`.

## 03 — Ingesta Oracle → Lake, 20 puntos (página 4)
Incremental por `fecha_creacion`/`updated_at`, schema evolution, no bloquear Oracle, Python Oracle, Parquet por año/mes, manejo de errores/reintentos, idempotencia, alternativa sin updated_at y patrón para cien tablas. **Fase 1:** generador/validador CSV y conversor Parquet sin prueba de runtime verificada; todavía no conector Oracle operativo.

## 04 — Modelo dimensional, 20 puntos (páginas 5-6)
`fact_ventas`, `fact_fx`, `fact_interacciones`, `dim_cliente`, `dim_tiempo`, `dim_sala`, `dim_fuente`, `dim_producto`; granularidad, claves naturales/sustitutas, SCD2 para cliente/sala, DDL, conversión fuente/mes, VPG sala/año, huérfanos, calendario fiscal e identidad multisistema. **Pendiente:** DDL, modelo, SQL y pruebas. Definición empresarial de VPG y denominadores comerciales no facilitada; consignar como supuestos, sin inventar definición corporativa.

## 05 — Airflow, 15 puntos (página 6)
DAG `airflow/dags/etl_sunset.py`; ingesta de leads y FX paralela → dbt → calidad → alertas Slack/correo; diario 02:00, TaskFlow/PythonOperator, retries/retry_delay/sla/email_on_failure y XCom; backfill seis meses, recursos y versionado. **Pendiente:** DAG, dbt y orquestación. Elegir versión de Airflow compatible con `sla` requerido o explicar alternativa para versión más reciente.

## 06 — Streaming, 15 puntos (página 7)
Productor Python; topics `transcripciones_raw`, `transcripciones_procesadas`, `alertas`; Spark Structured Streaming Kafka, regex de frases/compromisos, resultados PostgreSQL, checkpoint y semántica exactly-once, picos, orden y RAG. **Pendiente:** código y evidencias. `foreachBatch` puede reintentarse: garantizar idempotencia del sink con IDs y UPSERT; no prometer exactly-once físico extremo a extremo sin pruebas.

## 07 — Calidad y monitoreo, 10 puntos (página 8)
Completitud, unicidad, consistencia, frescura, integridad referencial, ≥5 pruebas en leads/FX usando dbt tests o Great Expectations; alertas/dashboard, política de cuarentena, drift y linaje. **Fase 1:** auditoría limitada sobre CSV, no framework de calidad productivo ni dashboard.

## 08 — Mantis, 10 puntos (páginas 8-9)
Leer comentarios Parquet, chunking, embeddings sentence-transformers/OpenAI, escritura incremental pgvector, cambio de versión de embeddings, privacidad y arquitectura RAG. **Pendiente:** módulo; sin API real de Mantis, documentar punto de integración propuesto, nunca certificar integración propietaria.

## 09 — Comunicación ejecutiva, 5 puntos (página 9)
Video de 5 minutos **o** cinco diapositivas explicando arquitectura, impacto en FX/Client Journey/finanzas/Mantis, riesgos, costos y supuestos. **Decisión del candidato ya expresada:** entregar ambos, sin presuponer puntos duplicados. Indicadores solo con ejecuciones verificables.

## 10 — Ingeniería y reproducibilidad, 5 puntos (páginas 1 y 10)
Código modular, `requirements.txt`, `.env.example`, Dockerfile, Compose, tests pytest y README con instrucciones. Requiere perfiles `demo` y `volume`, CI y pruebas evidenciadas, seguridad de secretos. **Fase 1:** código básico `unittest`, configuración parcial, valores fijos en generador, sin CI, Docker ni stack completo.

## Decisiones pendientes antes de comenzar la implementación de infraestructura

**A. Modalidad operativa/reproducibilidad:** elegir entre (1) demo liviana autocontenida + stack completo por perfiles Compose, (2) todo el stack pesado en un único `docker compose up`, o (3) infraestructura cloud real. Recomendación técnica: opción 1 para máxima reproducibilidad del evaluador con recursos variables.

**B. Fuentes reales:** el PDF menciona CSV proporcionados, pero la copia disponible no los acompaña. El usuario ya autorizó datos sintéticos: se documentará esta condición y se preservarán los campos de la prueba como contrato asumido.

**C. Métricas empresariales:** VPG y conversión dependen de definiciones de negocio no especificadas. Dejar consultas y supuestos explícitos y posponer decisiones empresariales definitivas hasta confirmación.

## Reglas de aceptación
- No marcar como `PASS` operaciones no ejecutadas: Oracle 11g real, Spark/Kafka/Docker, Parquet masivo y Mantis real pendientes.
- Toda prueba debe indicar comando, ambiente, tiempo, conteo origen/destino, error y artefacto de evidencia.
- No colocar secretos ni los ~21.4 millones de filas en el repositorio Git.
- Volumen declarado en JSON/YAML **no** equivale a volumen procesado.
