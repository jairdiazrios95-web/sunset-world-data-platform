# Correo de entrega — Sunset World (borrador listo para revisión)

**Asunto:** Entrega de prueba técnica — Data Engineer | Arquitectura y evidencia reproducible

Hola, Adrián y Alicia:

Comparto mi entrega de la prueba técnica para la posición de Data Engineer en Sunset World. Preparé una propuesta modular de integración de datos que abarca ingesta incremental y Parquet, modelado dimensional y dbt, orquestación con Airflow, streaming con Kafka/Spark, calidad y búsqueda semántica con pgvector.

Además de la documentación y el código, incluí evidencia reproducible de ejecución:

- **Integración completa:** cinco módulos tecnológicos y compuerta de aceptación aprobados en GitHub Actions (ejecución [#39336589227](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39336589227)).
- **Prueba de estrés relacional:** 42.8 millones de registros sintéticos cargados físicamente en PostgreSQL 16, en 436.102 segundos, sin registros huérfanos y con conteos estables tras el replay (ejecución [#39334943627](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39334943627)).

Dejo explícito el alcance: la prueba de gran volumen se realizó sobre PostgreSQL, mientras que las integraciones del resto de los componentes se validaron con muestras sintéticas más pequeñas. Los accesos empresariales a Oracle 11g y Mantis, así como algunas definiciones funcionales, se mantienen como supuestos por validar; no presento esas conexiones como realizadas.

**Repositorio privado:** https://github.com/jairdiazrios95-web/sunset-world-data-platform

Incluyo el documento de respuestas técnicas en PDF y la presentación ejecutiva de cinco diapositivas. El README contiene la arquitectura, instrucciones de reproducción, pruebas automatizadas, resultados y limitaciones.

Quedo atento a sus comentarios y a la oportunidad de explicar las decisiones de diseño y los resultados de manera detallada.

Saludos cordiales.

---

**Antes de enviar:** comprobar que los destinatarios tienen acceso efectivo al repositorio privado; no hacer público el código por conveniencia. Adjuntar los PDF/PPTX finales generados y verificar que corresponden al commit entregado. El correo aún no se ha enviado.
