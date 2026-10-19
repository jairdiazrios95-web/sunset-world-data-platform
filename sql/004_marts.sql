-- Idempotent snapshot integration from raw CSV landing to conformed dimensions/facts.
-- Source schemas are SYNTHETIC assumptions, not actual Sunset World schemas.
-- Execute after all ten raw tables are loaded. Fails on invalid casts / orphans.
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS ops.identity_crosswalk (
 source_system text NOT NULL, source_customer_id text NOT NULL,
 canonical_cliente_id text NOT NULL,
 match_method text NOT NULL DEFAULT 'authoritative_mapping',
 verified_at timestamptz,
 PRIMARY KEY(source_system,source_customer_id)
);
CREATE TABLE IF NOT EXISTS ops.rejected_record (
 rejection_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 source_name text NOT NULL, natural_key text, reason text NOT NULL,
 raw_payload jsonb, run_id text, rejected_at timestamptz NOT NULL DEFAULT now()
);

-- Fiscal calendar is initially calendar-year; customize fiscal start via config.
INSERT INTO dw.dim_tiempo(tiempo_key,fecha,anio,mes,trimestre,periodo_fiscal)
SELECT to_char(d,'YYYYMMDD')::integer, d::date, extract(year from d)::integer,
       extract(month from d)::integer, extract(quarter from d)::integer,
       concat('FY',extract(year from (d - (coalesce(nullif(current_setting('app.fiscal_start_month',true),''),'1')::integer-1) * interval '1 month'))::integer)
FROM generate_series(
 (SELECT min(dt) FROM (SELECT min(fecha_creacion::date) AS dt FROM raw.leads_ventas
                      UNION ALL SELECT min(fecha::date) FROM raw.fx_historico
                      UNION ALL SELECT min(fecha_tour::date) FROM raw.tours
                      UNION ALL SELECT min(fecha_comentario::date) FROM raw.comentarios) min_dates),
 (SELECT max(dt) FROM (SELECT max(fecha_creacion::date) AS dt FROM raw.leads_ventas
                      UNION ALL SELECT max(fecha::date) FROM raw.fx_historico
                      UNION ALL SELECT max(fecha_tour::date) FROM raw.tours
                      UNION ALL SELECT max(fecha_comentario::date) FROM raw.comentarios
                      UNION ALL SELECT max(nullif(fecha_venta,'')::date) FROM raw.leads_ventas) max_dates),
 interval '1 day') AS d
ON CONFLICT(tiempo_key) DO NOTHING;

INSERT INTO dw.dim_fuente(fuente_id,nombre_fuente)
SELECT fuente_id,nombre_fuente FROM raw.fuentes WHERE true
ON CONFLICT (fuente_id) DO UPDATE SET nombre_fuente=EXCLUDED.nombre_fuente;
INSERT INTO dw.dim_producto(producto_id,nombre_producto)
SELECT producto_id,nombre_producto FROM raw.productos WHERE true
ON CONFLICT(producto_id) DO UPDATE SET nombre_producto=EXCLUDED.nombre_producto;

-- SCD2: close only changed versions, and insert new versions. Historical
-- initial version starts at 1900-01-01; later changes start at ingestion time.
UPDATE dw.dim_cliente d SET valid_to=now(),is_current=false
FROM raw.clientes c WHERE d.cliente_id=c.cliente_id AND d.is_current
  AND d.segmento IS DISTINCT FROM c.segmento;
INSERT INTO dw.dim_cliente(cliente_id,segmento,valid_from,is_current)
SELECT c.cliente_id,c.segmento,
       CASE WHEN EXISTS (SELECT 1 FROM dw.dim_cliente hist WHERE hist.cliente_id=c.cliente_id)
            THEN now()::timestamp ELSE timestamp '1900-01-01' END,true
FROM raw.clientes c
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_cliente d
                  WHERE d.cliente_id=c.cliente_id AND d.is_current)
ON CONFLICT(cliente_id,valid_from) DO NOTHING;

UPDATE dw.dim_sala d SET valid_to=now(),is_current=false
FROM raw.salas s WHERE d.sala_id=s.sala_id AND d.is_current
  AND (d.destino,d.nombre_sala) IS DISTINCT FROM (s.destino,s.nombre_sala);
INSERT INTO dw.dim_sala(sala_id,destino,nombre_sala,valid_from,is_current)
SELECT s.sala_id,s.destino,s.nombre_sala,
       CASE WHEN EXISTS (SELECT 1 FROM dw.dim_sala h WHERE h.sala_id=s.sala_id)
            THEN now()::timestamp ELSE timestamp '1900-01-01' END,true
FROM raw.salas s
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_sala d WHERE d.sala_id=s.sala_id AND d.is_current)
ON CONFLICT(sala_id,valid_from) DO NOTHING;

INSERT INTO dw.fact_fx(fecha,moneda_origen,moneda_destino,tipo_cambio)
SELECT fecha::date,moneda_origen,moneda_destino,tipo_cambio::numeric
FROM raw.fx_historico WHERE true
ON CONFLICT(fecha,moneda_origen,moneda_destino)
DO UPDATE SET tipo_cambio=EXCLUDED.tipo_cambio;

CREATE TABLE IF NOT EXISTS dw.fact_tours (
 tour_id text PRIMARY KEY, lead_id text NOT NULL,
 cliente_key bigint NOT NULL REFERENCES dw.dim_cliente(cliente_key),
 sala_key bigint NOT NULL REFERENCES dw.dim_sala(sala_key),
 tiempo_key integer NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
 status_tour text NOT NULL
);

-- Halt instead of silently dropping orphan rows. Zero is required for release.
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM raw.leads_ventas l LEFT JOIN raw.clientes c USING(cliente_id)
            LEFT JOIN raw.salas s USING(sala_id)
            LEFT JOIN raw.fuentes f USING(fuente_id)
            LEFT JOIN raw.productos p USING(producto_id)
            WHERE c.cliente_id IS NULL OR s.sala_id IS NULL
               OR f.fuente_id IS NULL OR p.producto_id IS NULL) THEN
    RAISE EXCEPTION 'Orphan lead dimension found; halt mart publication';
 END IF;
 IF EXISTS (SELECT 1 FROM raw.tours t LEFT JOIN raw.leads_ventas l USING(lead_id)
            WHERE l.lead_id IS NULL OR l.cliente_id <> t.cliente_id) THEN
    RAISE EXCEPTION 'Orphan/inconsistent tour found';
 END IF;
 IF EXISTS (SELECT 1 FROM raw.leads_ventas l LEFT JOIN dw.fact_fx f
            ON f.fecha=l.fecha_venta::date AND f.moneda_origen=l.moneda
            AND f.moneda_destino='MXN'
            WHERE l.venta_id <> '' AND f.fecha IS NULL) THEN
    RAISE EXCEPTION 'FX missing for sales';
 END IF;
END $$;

INSERT INTO dw.fact_tours(tour_id,lead_id,cliente_key,sala_key,tiempo_key,status_tour)
SELECT t.tour_id,t.lead_id,c.cliente_key,s.sala_key,
       to_char(t.fecha_tour::date,'YYYYMMDD')::integer,t.status_tour
FROM raw.tours t
JOIN dw.dim_cliente c ON c.cliente_id=t.cliente_id AND t.fecha_tour::timestamp>=c.valid_from
                       AND (c.valid_to IS NULL OR t.fecha_tour::timestamp<c.valid_to)
JOIN dw.dim_sala s ON s.sala_id=t.sala_id AND t.fecha_tour::timestamp>=s.valid_from
                     AND (s.valid_to IS NULL OR t.fecha_tour::timestamp<s.valid_to)
WHERE true ON CONFLICT(tour_id) DO UPDATE SET
 lead_id=excluded.lead_id,cliente_key=excluded.cliente_key,
 sala_key=excluded.sala_key,tiempo_key=excluded.tiempo_key,status_tour=excluded.status_tour;

INSERT INTO dw.fact_ventas(venta_id,lead_id,tour_id,cliente_key,tiempo_key,sala_key,
 fuente_key,producto_key,moneda,monto_venta,monto_mxn)
SELECT l.venta_id,l.lead_id,nullif(l.tour_id,''),c.cliente_key,
       to_char(l.fecha_venta::date,'YYYYMMDD')::integer,s.sala_key,
       f.fuente_key,p.producto_key,l.moneda,l.monto_venta::numeric,
       round(l.monto_venta::numeric*x.tipo_cambio,2)
FROM raw.leads_ventas l
JOIN dw.dim_cliente c ON c.cliente_id=l.cliente_id AND l.fecha_venta::timestamp>=c.valid_from
                       AND (c.valid_to IS NULL OR l.fecha_venta::timestamp<c.valid_to)
JOIN dw.dim_sala s ON s.sala_id=l.sala_id AND l.fecha_venta::timestamp>=s.valid_from
                     AND (s.valid_to IS NULL OR l.fecha_venta::timestamp<s.valid_to)
JOIN dw.dim_fuente f ON f.fuente_id=l.fuente_id
JOIN dw.dim_producto p ON p.producto_id=l.producto_id
JOIN dw.fact_fx x ON x.fecha=l.fecha_venta::date AND x.moneda_origen=l.moneda AND x.moneda_destino='MXN'
WHERE nullif(l.venta_id,'') IS NOT NULL
ON CONFLICT(venta_id) DO UPDATE SET
 lead_id=EXCLUDED.lead_id,tour_id=EXCLUDED.tour_id,cliente_key=EXCLUDED.cliente_key,
 tiempo_key=EXCLUDED.tiempo_key,sala_key=EXCLUDED.sala_key,
 fuente_key=EXCLUDED.fuente_key,producto_key=EXCLUDED.producto_key,
 moneda=EXCLUDED.moneda,monto_venta=EXCLUDED.monto_venta,monto_mxn=EXCLUDED.monto_mxn;

INSERT INTO dw.fact_interacciones(interaccion_id,tour_id,cliente_key,tiempo_key,tipo,contenido,tiene_alerta)
SELECT r.comentario_id,r.tour_id,c.cliente_key,
       to_char(r.fecha_comentario::date,'YYYYMMDD')::integer,
       'comentario',r.texto,false
FROM raw.comentarios r
JOIN dw.dim_cliente c ON c.cliente_id=r.cliente_id AND r.fecha_comentario::timestamp>=c.valid_from
                       AND (c.valid_to IS NULL OR r.fecha_comentario::timestamp<c.valid_to)
WHERE true ON CONFLICT(interaccion_id) DO UPDATE SET
 tour_id=excluded.tour_id,cliente_key=excluded.cliente_key,
 tiempo_key=excluded.tiempo_key,contenido=excluded.contenido;

-- Optional provisional reporting views. KPI definitions pending business signoff.
CREATE OR REPLACE VIEW analytics.v_conversion_fuente_mes_provisional AS
SELECT f.nombre_fuente,date_trunc('month',l.fecha_creacion::date)::date AS mes,
       count(*) AS leads, count(*) FILTER (WHERE nullif(l.venta_id,'') IS NOT NULL) AS ventas,
       round(count(*) FILTER (WHERE nullif(l.venta_id,'') IS NOT NULL)::numeric
             /nullif(count(*),0),4) AS conversion_por_lead
FROM raw.leads_ventas l JOIN dw.dim_fuente f ON f.fuente_id=l.fuente_id
GROUP BY 1,2;
CREATE OR REPLACE VIEW analytics.v_vpg_sala_anio_provisional AS
WITH t AS (
 SELECT s.sala_key,extract(year from dt.fecha)::integer AS anio,
        count(DISTINCT ft.tour_id) AS tours
 FROM dw.fact_tours ft JOIN dw.dim_tiempo dt ON dt.tiempo_key=ft.tiempo_key
 JOIN dw.dim_sala s ON s.sala_key=ft.sala_key WHERE ft.status_tour='REALIZADO'
 GROUP BY 1,2
), v AS (
 SELECT sala_key,extract(year from dt.fecha)::integer AS anio,sum(monto_mxn) AS venta_mxn
 FROM dw.fact_ventas fv JOIN dw.dim_tiempo dt ON dt.tiempo_key=fv.tiempo_key GROUP BY 1,2
)
SELECT s.nombre_sala,t.anio,coalesce(v.venta_mxn,0) AS venta_mxn,t.tours,
       round(coalesce(v.venta_mxn,0)/nullif(t.tours,0),2) AS vpg_provisional_mxn
FROM t JOIN dw.dim_sala s USING(sala_key)
LEFT JOIN v ON v.sala_key=t.sala_key AND v.anio=t.anio;
