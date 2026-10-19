{{ config(materialized='view') }}
-- Explainable customer history features; no unsupported probability claim.
select c.cliente_id,
       count(f.venta_id) as total_compras,
       coalesce(sum(f.monto_mxn),0) as monto_total_mxn,
       max(t.fecha) as ultima_compra,
       (current_date - max(t.fecha))::int as dias_desde_ultima_compra
from dw.fact_ventas f
join dw.dim_cliente c on c.cliente_key=f.cliente_key
join dw.dim_tiempo t on t.tiempo_key=f.tiempo_key
group by c.cliente_id
