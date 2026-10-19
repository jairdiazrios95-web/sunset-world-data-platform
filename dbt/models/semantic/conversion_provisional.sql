{{ config(materialized='view') }}
-- Business definition not approved by Sunset; this is a demonstration only.
select nombre_fuente,mes,leads,ventas,conversion_por_lead
from analytics.v_conversion_fuente_mes_provisional
