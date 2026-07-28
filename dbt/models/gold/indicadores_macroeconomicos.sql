--Consolida indicadores macro mensais

WITH selic_mensal AS (
    SELECT
        TRUNC(data, 'MONTH') AS mes_ref,
        AVG(valor)              AS selic_media_mes,
        MAX(valor)              AS selic_max_mes,
        MIN(valor)              AS selic_min_mes
    FROM {{ source('silver', 'selic') }}
    GROUP BY 1
),
ipca_mensal AS (
    SELECT
        TRUNC(data, 'MONTH') AS mes_ref,
        SUM(valor)              AS ipca_acumulado_mes
    FROM {{ source('silver', 'ipca') }}
    GROUP BY 1
),
dollar_mensal AS (
    SELECT
        TRUNC(data, 'MONTH') AS mes_ref,
        AVG(valor)              AS dollar_medio_mes,
        MAX(valor)              AS dollar_max_mes,
        MIN(valor)              AS dollar_min_mes
    FROM {{ source('silver', 'dollar') }}
    GROUP BY 1
),
pib_mensal AS (
    SELECT
        TRUNC(data, 'MONTH') AS mes_ref,
        AVG(valor) AS pib_valor_mes
    FROM {{ source('silver', 'pib') }}
    GROUP BY 1
),
inadimplencia_mensal AS (
    SELECT
        TRUNC(data, 'MONTH') AS mes_ref,
        AVG(valor) AS inadimplencia_media_mes
    FROM {{ source('silver', 'inadimplencia') }}
    GROUP BY 1
)

SELECT
    s.mes_ref,
    s.selic_media_mes,
    s.selic_max_mes,
    s.selic_min_mes,
    i.ipca_acumulado_mes,
    d.dollar_medio_mes,
    d.dollar_max_mes,
    p.pib_valor_mes,
    n.inadimplencia_media_mes,
    ROUND(s.selic_media_mes - COALESCE(i.ipca_acumulado_mes, 0), 4) as juros_real_estimado
FROM selic_mensal s
LEFT JOIN ipca_mensal i ON s.mes_ref = i.mes_ref
LEFT JOIN dollar_mensal d ON s.mes_ref = d.mes_ref
LEFT JOIN pib_mensal p ON s.mes_ref = p.mes_ref
LEFT JOIN inadimplencia_mensal n ON s.mes_ref = n.mes_ref
ORDER BY 1 DESC

