# Layout sintético y relaciones — propuesta inicial

**No es un layout oficial de Sunset World**. El examen menciona dos nombres de CSV pero no proporciona sus columnas. En la pág. 2 da cantidades estimadas por sistema: Voltux ~4.5 M registros, TMS ~10 M comentarios y 6.9 M pagos. FX es diario. Por tanto, no asumimos que `leads_ventas.csv` tenga necesariamente 4.5 M filas en el sistema real.

| Archivo | Grano propuesto | Clave | Referencias |
|---|---|---|---|
| clientes.csv | cliente | cliente_id | — |
| leads_ventas.csv | lead (venta opcional) | lead_id | cliente_id, sala_id, fuente_id, producto_id, tour_id opcional, venta_id opcional |
| tours.csv | recorrido de venta | tour_id | lead_id, cliente_id, sala_id |
| contratos.csv | contrato de venta | contrato_id | venta_id, cliente_id |
| pagos.csv | pago individual | pago_id | contrato_id |
| comentarios.csv | comentario comercial | comentario_id | tour_id, cliente_id |
| fx_historico.csv | fecha + par de divisas | fecha, moneda_origen, moneda_destino | cruza ventas por fecha_venta y moneda |
| salas.csv | sala | sala_id | — |
| fuentes.csv | fuente de captación | fuente_id | — |
| productos.csv | producto | producto_id | — |

Fechas en ISO 8601; IDs alfanuméricos estables. Los importes y tipos de cambio son **ficticios** y no representan información real ni tasa histórica. No incluye información personal real. Se generan ventas solamente para algunos tours; las personas sin venta tienen `venta_id` vacío. Los contratos se asocian únicamente a ventas existentes; pagos y comentarios se vinculan a contratos y tours existentes, respectivamente.

**Dos métricas pendientes de validación de negocio:** conversión y VPG (Volume Per Guest, interpretación sectorial preliminar). El modelo conserva `tour_id`, `venta_id`, `fecha_venta`, `sala_id` y `monto_venta` para permitir sus cálculos cuando se acuerden denominadores, cortes temporales y exclusiones.

**Limitaciones deliberadas:** diseño sintético simplificado; no replica los esquemas físicos Oracle 11g, eventos tardíos, SCD2 ni ventas multicomponente. La corrida de gran volumen puede requerir mucho disco y tiempo; debe realizarse por separado. Para probar fallos, crear archivos de anomalías *aparte*, sin contaminar los datos correctos.
