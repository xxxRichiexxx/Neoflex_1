BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE MD_CURRENCY_D_TEMP AS
SELECT
    id::INT
    ,currency_rk::INT
    ,data_actual_date::DATE
    ,data_actual_end_date::DATE
    ,NULLIF(replace(currency_code, ' ', ''), '')::INT              AS currency_code
    ,NULLIF(replace(code_iso_char, ' ', ''), '')::CHAR(6)          AS code_iso_char
FROM 
    (SELECT
        *
        ,ROW_NUMBER() OVER (PARTITION BY currency_rk, data_actual_date ORDER BY id DESC) 
    FROM RAW.MD_CURRENCY_D
    WHERE id IS NOT NULL
        AND currency_rk IS NOT NULL 
        AND data_actual_date IS NOT NULL) c
WHERE row_number = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO DDS.MD_CURRENCY_D
(
    currency_rk
    ,data_actual_date
    ,data_actual_end_date
    ,currency_code
    ,code_iso_char
)
SELECT
    currency_rk
    ,data_actual_date
    ,data_actual_end_date
    ,currency_code
    ,code_iso_char
FROM MD_CURRENCY_D_TEMP
ON CONFLICT(currency_rk, data_actual_date)
DO UPDATE
SET 
    data_actual_end_date = EXCLUDED.data_actual_end_date
    ,currency_code = EXCLUDED.currency_code
    ,code_iso_char = EXCLUDED.code_iso_char;

-- Определение данных, которые не были вставлены в целевую таблицу слоя DDS/ODS и 
-- копирование этих данных в соответствующую таблицу слоя REJECTED_DATA
DROP TABLE IF EXISTS REJECTED_DATA.MD_CURRENCY_D;
CREATE TABLE REJECTED_DATA.MD_CURRENCY_D AS 
SELECT 
    r.*
FROM RAW.MD_CURRENCY_D r
LEFT JOIN (SELECT DISTINCT id FROM MD_CURRENCY_D_TEMP) t
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
    ,'DDS.MD_CURRENCY_D upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM MD_CURRENCY_D_TEMP;

COMMIT TRANSACTION;