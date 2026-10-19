-- Example only: fiscal year assumed calendar, conversion denominator = qualified tours.
-- Do not label as approved business KPI until Sunset validates definitions.
-- Conversion by source, year/month: one sale per lead within sample generator.
SELECT f.nombre_fuente, date_trunc('month',t.fecha) AS mes,
 COUNT(DISTINCT v.venta_id) AS ventas,
 SUM(v.monto_mxn) AS monto_mxn
FROM dw.fact_ventas v JOIN dw.dim_fuente f ON f.fuente_key=v.fuente_key
JOIN dw.dim_tiempo t ON t.tiempo_key=v.tiempo_key
GROUP BY 1,2 ORDER BY 2,1;
-- Illustrative VPG = gross sales / completed tours. Numerator is available
-- from fact_ventas; denominator requires independently loaded tour grain.
-- Report as provisional ONLY; not a definitive corporate VPG definition.
-- Orphan fact FKs should always return zero rows when constraints are enabled.
SELECT v.venta_id FROM dw.fact_ventas v
LEFT JOIN dw.dim_cliente c ON c.cliente_key=v.cliente_key
WHERE c.cliente_key IS NULL;
