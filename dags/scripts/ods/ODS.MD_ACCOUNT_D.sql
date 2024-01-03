BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE MD_ACCOUNT_D_TEMP AS
SELECT
    a.id::INT
    ,a.data_actual_date::DATE
    ,a.data_actual_end_date::DATE
    ,a.account_rk::INT
    ,a.account_number::NUMERIC(40)
    ,a.char_type::CHAR(1)
    ,a.currency_rk::INT
    ,a.currency_code::INT
FROM 
    (SELECT 
        *
        ,ROW_NUMBER() OVER (PARTITION BY DATA_ACTUAL_DATE, ACCOUNT_RK ORDER BY id DESC)
    FROM RAW.MD_ACCOUNT_D
    WHERE id IS NOT NULL
        AND data_actual_date IS NOT NULL 
        AND data_actual_end_date IS NOT NULL
        AND account_rk IS NOT NULL 
        AND char_type IS NOT NULL 
        AND currency_rk IS NOT NULL 
        AND currency_code IS NOT NULL) a
WHERE row_number = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO ODS.MD_ACCOUNT_D
(
	data_actual_date
	,data_actual_end_date
    ,account_rk
	,account_number
	,char_type
	,currency_rk
	,currency_code
)
SELECT
	data_actual_date
	,data_actual_end_date
    ,account_rk
	,account_number
	,char_type
	,currency_rk
	,currency_code
FROM MD_ACCOUNT_D_TEMP
ON CONFLICT(DATA_ACTUAL_DATE, ACCOUNT_RK)
DO UPDATE
SET 
	data_actual_end_date = EXCLUDED.data_actual_end_date
	,account_number = EXCLUDED.account_number
	,char_type = EXCLUDED.char_type
	,currency_rk = EXCLUDED.currency_rk
	,currency_code = EXCLUDED.currency_code;

-- Запись логов в соответствующую таблицу
INSERT INTO LOGS.DAG_LOGS
(
    dag_id
    ,info
    ,ts
)
SELECT 
    '{{dag.dag_id}}'
    ,'ODS.MD_ACCOUNT_D upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM MD_ACCOUNT_D_TEMP;

COMMIT TRANSACTION;