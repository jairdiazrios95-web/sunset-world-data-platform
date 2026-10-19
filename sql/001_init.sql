-- Synthetic example star schema. Schemas are not actual Sunset World layouts.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS ai;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
 tiempo_key integer PRIMARY KEY, fecha date UNIQUE NOT NULL, anio integer NOT NULL,
 mes integer NOT NULL, trimestre integer NOT NULL, periodo_fiscal text NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_fuente (
 fuente_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 fuente_id text UNIQUE NOT NULL, nombre_fuente text NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_producto (
 producto_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 producto_id text UNIQUE NOT NULL, nombre_producto text NOT NULL
);
CREATE TABLE IF NOT EXISTS dw.dim_cliente (
 cliente_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 cliente_id text NOT NULL, segmento text,
 valid_from timestamp NOT NULL, valid_to timestamp,
 is_current boolean NOT NULL DEFAULT true,
 UNIQUE (cliente_id, valid_from), CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cliente_actual ON dw.dim_cliente(cliente_id) WHERE is_current;
CREATE TABLE IF NOT EXISTS dw.dim_sala (
 sala_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 sala_id text NOT NULL, destino text, nombre_sala text,
 valid_from timestamp NOT NULL, valid_to timestamp,
 is_current boolean NOT NULL DEFAULT true,
 UNIQUE (sala_id,valid_from), CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sala_actual ON dw.dim_sala(sala_id) WHERE is_current;
CREATE TABLE IF NOT EXISTS dw.fact_ventas (
 venta_id text PRIMARY KEY, lead_id text NOT NULL, tour_id text,
 cliente_key bigint NOT NULL REFERENCES dw.dim_cliente(cliente_key),
 tiempo_key integer NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
 sala_key bigint NOT NULL REFERENCES dw.dim_sala(sala_key),
 fuente_key bigint REFERENCES dw.dim_fuente(fuente_key),
 producto_key bigint REFERENCES dw.dim_producto(producto_key),
 moneda text NOT NULL, monto_venta numeric(18,2) NOT NULL,
 monto_mxn numeric(18,2),
 created_at timestamp NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ventas_fecha_sala ON dw.fact_ventas(tiempo_key,sala_key);
CREATE TABLE IF NOT EXISTS dw.fact_fx (
 fecha date NOT NULL, moneda_origen text NOT NULL, moneda_destino text NOT NULL,
 tipo_cambio numeric(18,8) NOT NULL CHECK(tipo_cambio>0),
 PRIMARY KEY(fecha,moneda_origen,moneda_destino)
);
CREATE TABLE IF NOT EXISTS dw.fact_interacciones (
 interaccion_id text PRIMARY KEY, tour_id text, cliente_key bigint REFERENCES dw.dim_cliente(cliente_key),
 tiempo_key integer REFERENCES dw.dim_tiempo(tiempo_key), tipo text NOT NULL,
 contenido text NOT NULL, tiene_alerta boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS ops.ingestion_watermark (
 source_name text PRIMARY KEY, last_committed timestamp, last_run_id text,
 updated_at timestamp NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ops.batch_audit (
 run_id text NOT NULL, source_name text NOT NULL, extracted bigint NOT NULL,
 loaded bigint NOT NULL, rejected bigint NOT NULL, elapsed_seconds numeric,
 status text NOT NULL, started_at timestamptz DEFAULT now(),
 PRIMARY KEY(run_id,source_name)
);
-- Store vectors only after confirming embedding dimension for the chosen model.
CREATE TABLE IF NOT EXISTS ai.document_chunk (
 chunk_id text PRIMARY KEY, source_id text NOT NULL, source_type text NOT NULL,
 content text NOT NULL, content_hash text NOT NULL, model_id text NOT NULL,
 embedding vector(384) NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(source_id,source_type,content_hash,model_id)
);
CREATE INDEX IF NOT EXISTS idx_chunk_source ON ai.document_chunk(source_type,source_id);
