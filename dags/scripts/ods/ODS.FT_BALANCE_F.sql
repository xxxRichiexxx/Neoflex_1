BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE FT_BALANCE_F_TEMP AS
SELECT
    b.id::INT                                                
    ,TO_DATE(b.on_date, 'DD.MM.YYYY')       AS on_date
    ,b.account_rk::INT
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
WHERE row_number = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO ODS.FT_BALANCE_F
(
    on_date
    ,account_rk
    ,currency_rk
    ,balance_out
)
SELECT
    on_date
    ,account_rk
    ,currency_rk
    ,balance_out
FROM FT_BALANCE_F_TEMP
ON CONFLICT(ON_DATE, ACCOUNT_RK)
DO UPDATE
SET 
    currency_rk = EXCLUDED.currency_rk
    ,balance_out = EXCLUDED.balance_out;

-- Запись логов в соответствующую таблицу
INSERT INTO LOGS.DAG_LOGS
(
    dag_id
    ,info
    ,ts
)
SELECT 
    '{{dag.dag_id}}'
    ,'ODS.FT_BALANCE_F upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM FT_BALANCE_F_TEMP;

COMMIT TRANSACTION;