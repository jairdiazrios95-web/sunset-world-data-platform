# Observabilidad: paneles y alertas operativas

**Panel mínimo de 4 módulos (Metabase/Grafana a elección según infraestructura):** (1) `ops.batch_audit` con filas extraídas/cargadas/rechazadas, duración, estado y SLA por fuente; (2) `ops.ingestion_watermark` para edad de fuente y lag; (3) calidad por fecha, errores críticos y porcentaje de completitud, con webhook/red de on-call; (4) `ops.call_center_alert` con alertas por categoría y tiempo, sin transcript ni PII en dashboards públicos.

SQL de ejemplo para panel del estado de cargas (requiere cargas reales y registro en ops):

```sql
SELECT date_trunc('day',started_at) AS dia,source_name,
 sum(extracted) AS registros_extraidos,sum(loaded) AS registros_cargados,
 sum(rejected) AS rechazados,
 round(avg(elapsed_seconds),2) AS segundos_promedio,
 count(*) FILTER (WHERE status <> 'PASS') AS fallos
FROM ops.batch_audit
GROUP BY 1,2 ORDER BY 1 DESC,2;
```

**Gates:** ausencia de fuente; esquema alterado incompatible; FK huérfana; precio/tasa <=0; manifest incongruente -> BLOCK. Datos históricos sintéticos fuera de SLA -> WARNING documentado, no fallo; producción según SLA. Los avisos técnicos se disparan desde el callback Airflow y `sunset.quality`; la efectividad del webhook/SMTP necesita prueba de integración.

**Drift:** `python -m sunset.drift --reference baseline.csv --current actual.csv --column fuente_id` obtiene PSI; cambios de mezcla de sala/fuente/producto pueden ser estacionales. Los umbrales requieren baseline y acuerdo de negocio.

**SLO propuestos a negociar:** éxito de pipeline, frescura, p95 de entrega, disponibilidad del vector index, y costo por millón de registros. No hay evidencia para fijar SLA definitivo ni pronóstico de costo.

**Demo Grafana opcional:** `docker compose --profile monitoring up -d grafana` y abrir http://localhost:3000. Dashboard JSON versionado en `monitoring/dashboards/sunset.json`. La carga de panel requiere que exista la tabla `ops.batch_audit` en PostgreSQL, y se valida de forma real en un equipo con Docker.
