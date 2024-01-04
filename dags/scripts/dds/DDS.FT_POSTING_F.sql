BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE FT_POSTING_F_TEMP AS
SELECT
    p.id::INT
	,p.oper_date::DATE
    ,ca.id                          AS credit_account_id
	,p.credit_account_rk::INT
    ,da.id                          AS debet_account_id
	,p.debet_account_rk::INT
	,p.credit_amount::NUMERIC(9,3)
	,p.debet_amount::NUMERIC(9,3)
FROM 
    (SELECT 
        *
        ,ROW_NUMBER() OVER (PARTITION BY OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK ORDER BY id DESC)
    FROM RAW.FT_POSTING_F
    WHERE id IS NOT NULL 
        AND oper_date IS NOT NULL 
        AND credit_account_rk IS NOT NULL
        AND debet_account_rk IS NOT NULL
        AND credit_amount IS NOT NULL 
        AND debet_amount IS NOT NULL
        ) p
INNER JOIN DDS.MD_ACCOUNT_D ca 
    ON p.credit_account_rk::INT = ca.account_rk
        AND p.oper_date::DATE BETWEEN ca.data_actual_date AND ca.data_actual_end_date
INNER JOIN DDS.MD_ACCOUNT_D da 
    ON p.debet_account_rk::INT = da.account_rk
        AND p.oper_date::DATE BETWEEN da.data_actual_date AND da.data_actual_end_date
WHERE row_number = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO DDS.FT_POSTING_F
(
	oper_date
    ,credit_account_id
	,credit_account_rk
    ,debet_account_id
	,debet_account_rk
	,credit_amount
	,debet_amount
)
SELECT
	oper_date
    ,credit_account_id
	,credit_account_rk
    ,debet_account_id
	,debet_account_rk
	,credit_amount
	,debet_amount
FROM FT_POSTING_F_TEMP
ON CONFLICT(OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK)
DO UPDATE
SET 
	credit_amount = EXCLUDED.credit_amount
	,debet_amount = EXCLUDED.debet_amount;

-- Определение данных, которые не были вставлены в целевую таблицу слоя DDS/ODS и 
-- копирование этих данных в соответствующую таблицу слоя REJECTED_DATA
DROP TABLE IF EXISTS REJECTED_DATA.FT_POSTING_F;
CREATE TABLE REJECTED_DATA.FT_POSTING_F AS 
SELECT 
    r.*
FROM RAW.FT_POSTING_F r
LEFT JOIN (SELECT DISTINCT id FROM FT_POSTING_F_TEMP) t
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
    ,'DDS.FT_POSTING_F upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM FT_POSTING_F_TEMP;

COMMIT TRANSACTION;