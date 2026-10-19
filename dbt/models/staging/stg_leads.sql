{{ config(materialized='view') }}
select lead_id,cliente_id,fuente_id,sala_id,producto_id,
       fecha_creacion::date as fecha_creacion,
       nullif(venta_id,'') as venta_id,
       nullif(tour_id,'') as tour_id,
       nullif(monto_venta,'')::numeric as monto_venta,
       moneda,estado_lead
from raw.leads_ventas
