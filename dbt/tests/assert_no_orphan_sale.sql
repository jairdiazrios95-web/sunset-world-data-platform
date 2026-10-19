select l.venta_id from {{ ref('stg_leads') }} l
left join dw.fact_ventas f on f.venta_id=l.venta_id
where l.venta_id is not null and f.venta_id is null
