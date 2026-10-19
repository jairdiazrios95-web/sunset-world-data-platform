{{ config(materialized='view') }}
select s.destino, s.nombre_sala,dt.anio,dt.mes,
       count(distinct f.venta_id) as ventas,
       sum(f.monto_mxn) as ingresos_mxn
from dw.fact_ventas f
join dw.dim_sala s on f.sala_key=s.sala_key
join dw.dim_tiempo dt on f.tiempo_key=dt.tiempo_key
group by 1,2,3,4
