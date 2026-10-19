# Arquitectura técnica / decisiones

**Aprobada opción A:** docker-compose con perfiles modulares, no todos los servicios al inicio. CPU y memoria limitados en estaciones de evaluadores.

- Datos sintéticos deterministas: CSV fuente, luego Parquet columnar año/mes. Datos reales Oracle 11g NO disponibles; adaptador Thick (pendiente prueba de conexión).
- Modelo estrella Postgres con dimensiones SCD2 de sala/cliente; dimensiones con vigencias temporales y FK a la versión aplicable en ETL (loader pendiente).
- Airflow 2.10.x TaskFlow, 02:00, `catchup=False` para evitar backfills accidentales. Backfill seis meses por intervalos y límite de simultaneidad, no por un único trabajo gigante.
- `foreachBatch` Kafka→Postgres implementa idempotencia mediante clave estable. No equivale a garantía exactly-once para todos los sistemas.
- Con pgvector se integra una **interfaz propuesta** para Mantis. No se conoce API propietaria ni se ha probado comunicación con Mantis real.
- `fact_fx` es fact de tasas diarias, no transaccional monetario. Métricas de conversión y VPG dependen de contrato de negocio no entregado.
- CSV originales (`leads_ventas.csv`, `fx_historico.csv`) no venían adjuntos; se usan layouts asumidos, nunca oficiales.
- Para evitar fuga de datos, `.env` está ignorado. No se deben introducir datos reales sin autorización.
- 21.4 millones son referencias de cardinalidad; las pruebas de escala aún no se han ejecutado.
