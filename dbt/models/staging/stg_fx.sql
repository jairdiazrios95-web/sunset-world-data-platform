{{ config(materialized='view') }}
select fecha, moneda_origen, moneda_destino, tipo_cambio
from dw.fact_fx
