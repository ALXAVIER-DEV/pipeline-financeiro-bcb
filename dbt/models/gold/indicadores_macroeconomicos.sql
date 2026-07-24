--Consolida indicadores macro mensais

WITH selic_mensal AS (
    SELECT
        DATE_TRUNC(data, MONTH) AS mes_ref,
        AVG(valor)              AS selic_media_mes,
        MAX(valor)              AS selic_max_mes,
        MIN(valor)              AS selic_min_mes
    FROM {{ source('silver', 'selic') }}
    GROUP BY 1
),
ipca_mensal AS (
    SELECT
        DATE_TRUNC(data, MONTH) AS mes_ref,
        SUM(valor)              AS ipca_acumulado_mes
    FROM {{ source('silver', 'ipca') }}
    GROUP BY 1
),
dolar_mensal AS (
    SELECT
        DATE_TRUNC(data, MONTH) AS mes_ref,
        AVG(valor)              AS dolar_medio_mes,
        MAX(valor)              AS dolar_max_mes,
        MIN(valor)              AS dolar_min_mes
    FROM {{ source('silver', 'dolar') }}
    GROUP BY 1
)

SELECT
    s.mes_ref,
    s.selic_media_mes,
    s.selic_max_mes,
    s.selic_min_mes,
    i.ipca_acumulado_mes,
    d.dolar_medio_mes,
    d.dolar_max_mes,
    ROUND(s.selic_media_mes - COALESCE(i.ipca_acumulado_mes, 0), 4) as juros_real_estimado
FROM selic_mensal s
LEFT JOIN ipca_mensal i ON s.mes_ref = i.mes_ref
LEFT JOIN dolar_mensal d ON s.mes_ref = d.mes_ref
ORDER BY 1 DESC

