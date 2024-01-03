BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE MD_ACCOUNT_D_TEMP AS
WITH 
ac_cur AS
    (
        SELECT
            a.id::INT
            ,GREATEST(a.data_actual_date::DATE, c.data_actual_date)         AS data_actual_date
            ,LEAST(a.data_actual_end_date::DATE, c.data_actual_end_date)    AS data_actual_end_date
            ,a.account_rk::INT
            ,a.account_number
            ,a.char_type::CHAR(1)
            ,c.id                                                           AS currency_id
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
        INNER JOIN DDS.MD_CURRENCY_D c 
            ON a.currency_rk::INT = c.currency_rk
                AND a.data_actual_date::DATE < c.data_actual_end_date
                AND a.data_actual_end_date::DATE > c.data_actual_date
        WHERE row_number = 1
    )
SELECT
    ac_cur.id
    ,GREATEST(ac_cur.data_actual_date::DATE, l.start_date)                  AS data_actual_date
    ,LEAST(ac_cur.data_actual_end_date::DATE, l.end_date)                   AS data_actual_end_date
    ,ac_cur.account_rk
    ,ac_cur.account_number::NUMERIC(40)
    ,l.id                                                                   AS ledger_account_id
    ,ac_cur.char_type
    ,ac_cur.currency_id
    ,ac_cur.currency_rk
    ,ac_cur.currency_code
FROM ac_cur
INNER JOIN DDS.MD_LEDGER_ACCOUNT_S AS l
    ON SUBSTRING(ac_cur.account_number, 1, 5)::INT = l.ledger_account
        AND ac_cur.data_actual_date < l.end_date
        AND ac_cur.data_actual_end_date > l.start_date;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO DDS.MD_ACCOUNT_D
(
	data_actual_date
	,data_actual_end_date
    ,account_rk
	,account_number
    ,ledger_account_id
	,char_type
    ,currency_id
	,currency_rk
	,currency_code
)
SELECT
	data_actual_date
	,data_actual_end_date
    ,account_rk
	,account_number
    ,ledger_account_id
	,char_type
    ,currency_id
	,currency_rk
	,currency_code
FROM MD_ACCOUNT_D_TEMP
ON CONFLICT(DATA_ACTUAL_DATE, ACCOUNT_RK)
DO UPDATE
SET 
	data_actual_end_date = EXCLUDED.data_actual_end_date
	,account_number = EXCLUDED.account_number
	,char_type = EXCLUDED.char_type
    ,currency_id = EXCLUDED.currency_id
	,currency_rk = EXCLUDED.currency_rk
	,currency_code = EXCLUDED.currency_code;

-- Определение данных, которые не были вставлены в целевую таблицу слоя DDS/ODS и 
-- копирование этих данных в соответствующую таблицу слоя REJECTED_DATA
DROP TABLE IF EXISTS REJECTED_DATA.MD_ACCOUNT_D;
CREATE TABLE REJECTED_DATA.MD_ACCOUNT_D AS 
SELECT 
    r.*
FROM RAW.MD_ACCOUNT_D r
LEFT JOIN (SELECT DISTINCT id FROM MD_ACCOUNT_D_TEMP) t
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
    ,'DDS.MD_ACCOUNT_D upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM MD_ACCOUNT_D_TEMP;

COMMIT TRANSACTION;