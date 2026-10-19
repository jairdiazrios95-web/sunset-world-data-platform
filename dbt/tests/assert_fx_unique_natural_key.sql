select fecha,moneda_origen,moneda_destino
from {{ ref('stg_fx') }}
group by 1,2,3 having count(*) > 1
