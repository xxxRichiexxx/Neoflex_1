BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE MD_EXCHANGE_RATE_D_TEMP AS
SELECT
    r.id::INT
    ,GREATEST(r.data_actual_date::DATE, c.data_actual_date)         AS data_actual_date
    ,LEAST(r.data_actual_end_date::DATE, c.data_actual_end_date)    AS data_actual_end_date
    ,r.currency_rk::INT
    ,c.id                                                           AS currency_id
    ,r.reduced_cource::NUMERIC(8,3)
    ,r.code_iso_num::INT
FROM
	(SELECT
		* 
		,ROW_NUMBER() OVER (PARTITION BY DATA_ACTUAL_DATE, CURRENCY_RK ORDER BY id DESC)
	FROM RAW.MD_EXCHANGE_RATE_D
    WHERE id IS NOT NULL
        AND currency_rk IS NOT NULL) r
INNER JOIN DDS.MD_CURRENCY_D c 
	ON r.currency_rk::INT = c.currency_rk
		AND r.data_actual_date::DATE < c.data_actual_end_date
        AND r.data_actual_end_date::DATE > c.data_actual_date
WHERE r.ROW_NUMBER = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO DDS.MD_EXCHANGE_RATE_D
(
    data_actual_date
    ,data_actual_end_date
    ,currency_rk
    ,currency_id
    ,reduced_cource
    ,code_iso_num
)
SELECT
    data_actual_date
    ,data_actual_end_date
    ,currency_rk
    ,currency_id
    ,reduced_cource
    ,code_iso_num
FROM MD_EXCHANGE_RATE_D_TEMP
ON CONFLICT(DATA_ACTUAL_DATE, CURRENCY_RK)
DO UPDATE 
SET 
    data_actual_end_date = EXCLUDED.data_actual_end_date
    ,reduced_cource = EXCLUDED.reduced_cource
    ,code_iso_num = EXCLUDED.code_iso_num;

-- Определение данных, которые не были вставлены в целевую таблицу слоя DDS/ODS и 
-- копирование этих данных в соответствующую таблицу слоя REJECTED_DATA
DROP TABLE IF EXISTS REJECTED_DATA.MD_EXCHANGE_RATE_D;
CREATE TABLE REJECTED_DATA.MD_EXCHANGE_RATE_D AS 
SELECT 
    r.*
FROM RAW.MD_EXCHANGE_RATE_D r
LEFT JOIN (SELECT DISTINCT id FROM MD_EXCHANGE_RATE_D_TEMP) t
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
    ,'DDS.MD_EXCHANGE_RATE_D upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM MD_EXCHANGE_RATE_D_TEMP;

COMMIT TRANSACTION;