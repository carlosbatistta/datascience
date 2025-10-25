{{ config(schema="prata", materialized="table") }}

WITH tesouro_limpo AS (
    SELECT
        CAST(data AS DATE) AS data,
        CAST(valor AS NUMERIC(15, 3)) AS rendimento
    FROM {{ source('bronze', 'tesouro') }}
    WHERE data IS NOT NULL AND valor IS NOT NULL
)

SELECT
    data,
    rendimento
FROM tesouro_limpo
