BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE MD_EXCHANGE_RATE_D_TEMP AS
SELECT
    r.id::INT
    ,r.data_actual_date::DATE
    ,r.data_actual_end_date::DATE
    ,r.currency_rk::INT
    ,r.reduced_cource::NUMERIC(8,3)
    ,r.code_iso_num::INT
FROM
	(SELECT
		* 
		,ROW_NUMBER() OVER (PARTITION BY DATA_ACTUAL_DATE, CURRENCY_RK ORDER BY id DESC)
	FROM RAW.MD_EXCHANGE_RATE_D
    WHERE id IS NOT NULL
        AND currency_rk IS NOT NULL) r
WHERE r.ROW_NUMBER = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO ODS.MD_EXCHANGE_RATE_D
(
    data_actual_date
    ,data_actual_end_date
    ,currency_rk
    ,reduced_cource
    ,code_iso_num
)
SELECT
    data_actual_date
    ,data_actual_end_date
    ,currency_rk
    ,reduced_cource
    ,code_iso_num
FROM MD_EXCHANGE_RATE_D_TEMP
ON CONFLICT(DATA_ACTUAL_DATE, CURRENCY_RK)
DO UPDATE 
SET 
    data_actual_end_date = EXCLUDED.data_actual_end_date
    ,reduced_cource = EXCLUDED.reduced_cource
    ,code_iso_num = EXCLUDED.code_iso_num;

-- Запись логов в соответствующую таблицу
INSERT INTO LOGS.DAG_LOGS
(
    dag_id
    ,info
    ,ts
)
SELECT 
    '{{dag.dag_id}}'
    ,'ODS.MD_EXCHANGE_RATE_D upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM MD_EXCHANGE_RATE_D_TEMP;

COMMIT TRANSACTION;