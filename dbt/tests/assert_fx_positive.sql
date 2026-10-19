select * from {{ ref('stg_fx') }} where tipo_cambio <= 0
