BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE FT_BALANCE_F_TEMP AS
SELECT
    b.id::INT                                                
    ,TO_DATE(b.on_date, 'DD.MM.YYYY')       AS on_date
    ,a.id                                   AS account_id
    ,b.account_rk::INT
    ,c.id                                   AS currency_id
    ,b.currency_rk::INT
    ,b.balance_out::NUMERIC(9,3)
FROM 
    (SELECT 
        *
        ,ROW_NUMBER() OVER (PARTITION BY ON_DATE, ACCOUNT_RK ORDER BY id DESC)
    FROM RAW.FT_BALANCE_F
    WHERE id IS NOT NULL 
        AND on_date IS NOT NULL 
        AND account_rk IS NOT NULL 
        AND balance_out IS NOT NULL) b
INNER JOIN DDS.MD_ACCOUNT_D a 
    ON b.account_rk::INT = a.account_rk
    AND TO_DATE(b.on_date, 'DD.MM.YYYY') BETWEEN a.data_actual_date AND a.data_actual_end_date
INNER JOIN DDS.MD_CURRENCY_D c
    ON b.currency_rk::INT = c.currency_rk
    AND TO_DATE(b.on_date, 'DD.MM.YYYY') BETWEEN c.data_actual_date AND c.data_actual_end_date
WHERE row_number = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO DDS.FT_BALANCE_F
(
    on_date
    ,account_id
    ,account_rk
    ,currency_rk
    ,balance_out
)
SELECT
    on_date
    ,account_id
    ,account_rk
    ,currency_rk
    ,balance_out
FROM FT_BALANCE_F_TEMP
ON CONFLICT(ON_DATE, ACCOUNT_RK)
DO UPDATE
SET 
    currency_rk = EXCLUDED.currency_rk
    ,balance_out = EXCLUDED.balance_out;

-- Определение данных, которые не были вставлены в целевую таблицу слоя DDS/ODS и 
-- копирование этих данных в соответствующую таблицу слоя REJECTED_DATA
DROP TABLE IF EXISTS REJECTED_DATA.FT_BALANCE_F;
CREATE TABLE REJECTED_DATA.FT_BALANCE_F AS
SELECT 
    r.*
FROM RAW.FT_BALANCE_F r
LEFT JOIN (SELECT DISTINCT id FROM FT_BALANCE_F_TEMP) t
    ON r.id::INT = t.id 
WHERE t.id IS NULL;

-- Запись логов в соответствующую таблицу
INSERT INTO LOGS.DAG_LOGS
(
    dag_id
    ,info
    ,ts
)
SELECT 
    '{{dag.dag_id}}'
    ,'DDS.FT_BALANCE_F upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM FT_BALANCE_F_TEMP;

COMMIT TRANSACTION;