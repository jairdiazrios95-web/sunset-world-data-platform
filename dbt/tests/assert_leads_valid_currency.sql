select * from {{ ref('stg_leads') }} where moneda not in ('MXN','USD')
