# V0.6: Pruebas sobre motores reales — criterios de aceptación

**Estado: infraestructura externa pendiente de ejecución.** Esta versión prepara
pruebas con motores reales; no contiene resultados inventados de esas herramientas.
Los reportes `reports/stress_*_verified.json` conservados de V0.5 son pruebas
anteriores contra datos sintéticos/SQLite y no deben presentarse como benchmark Spark.

## Rutas de CI/CD añadidas

| GitHub Actions | Motor real bajo prueba | Prueba de aceptación | Estado al empacar |
|---|---|---|---|
| `live-platform.yml` | PyArrow 19 + PostgreSQL 16/pgvector + dbt | CSV→Parquet→raw→modelo estrella→dbt; carga y replay, 10 fuentes y conteos | No ejecutada |
| `live-minio.yml` | MinIO S3 + PyArrow | PUT/GET objetos Parquet, SHA-256 bytes idénticos, conteos y replay | No ejecutada |
| `live-streaming.yml` | Apache Kafka 3.9 + Spark 3.5 + PostgreSQL | 5 mensajes, 2 alertas, eventos en Kafka y PG, replay sin filas duplicadas | No ejecutada |
| `live-airflow.yml` | Apache Airflow 2.10.5 + PostgreSQL + dbt | DAG real `airflow dags test` completada, calidad PASS | No ejecutada |
| `live-rag.yml` | sentence-transformers + pgvector | Descarga modelo, generación de embeddings reales, búsqueda semántica, replay sin recálculo | No ejecutada |

Al ejecutar en GitHub Actions, un `PASS` requiere que el trabajo termine sin error
**y que el JSON de evidencia se haya publicado como artifact**. Que un workflow esté
versionado no equivale a que haya pasado. Fallas de red, imágenes Docker, cambios
de dependencias o servicios con recursos limitados deben registrarse como fallos.

## Cómo utilizarlo sin instalar nada en la PC

1. Subir **el contenido del ZIP** a un repositorio GitHub (no el ZIP como archivo único).
2. Acceder a **Actions**, ejecutar manualmente los flujos anteriores cuando corresponda.
3. Revisar el resultado del job y descargar el artifact de evidencia JSON.
4. Ante un fallo, corregir el código y repetir; *nunca* cambiar FAIL a PASS manualmente.
5. Consolidar JSON, logs, tiempos y capturas verificables en las diapositivas y video.

El flujo `live-platform` y `live-minio` también se ejecutan con push a `main`,
para dar una señal temprana de errores. Airflow/Kafka/RAG son manuales por su
alto consumo de memoria, descargas y duración. Para 42.8 M en PostgreSQL/Spark se
requiere un entorno dimensionado; los runners estándar no son una garantía de
capacidad ni sustituyen pruebas de resistencia, estrés, fallos de red y SLO.

## Oracle 11g: detalle que no se puede omitir

`python-oracledb` Thin no soporta Oracle 11g; Oracle 11.2 requiere **Thick**
con **Oracle Client 19** compatible, versión de driver controlada en
`requirements-oracle11g.txt` y conectividad/autorización al Oracle verdadero.
Para Oracle 11.1 hay que confirmar la versión del servidor y la matriz de
compatibilidad antes de elegir cliente. No tenemos servidor ni credenciales,
por lo que **no se afirma prueba Oracle real**. El adaptador CSV valida
contratos, NO una conexión Oracle 11g.

Documentación oficial:
- https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html
- https://docs.github.com/en/actions/tutorials/use-containerized-services/create-postgresql-service-containers
- https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html
- https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
- https://arrow.apache.org/docs/python/install.html

## Criterio de producción

Nunca se puede garantizar riesgo de fallo **cero** ante hardware, red, credenciales,
esquemas productivos desconocidos o datos distintos de los sintéticos. Sí es posible
exigir evidencia por entorno, pruebas negativas, recuperación de checkpoint,
conciliación, límites de carga medidos y plan de reversión. Declarar un servicio
"probado" únicamente después de ejecutar un entorno que efectivamente lo incluye.
