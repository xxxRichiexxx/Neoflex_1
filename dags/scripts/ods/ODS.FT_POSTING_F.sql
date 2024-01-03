BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE FT_POSTING_F_TEMP AS
SELECT
    p.id::INT
	,p.oper_date::DATE
	,p.credit_account_rk::INT
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
WHERE row_number = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO ODS.FT_POSTING_F
(
	oper_date
	,credit_account_rk
	,debet_account_rk
	,credit_amount
	,debet_amount
)
SELECT
	oper_date
	,credit_account_rk
	,debet_account_rk
	,credit_amount
	,debet_amount
FROM FT_POSTING_F_TEMP
ON CONFLICT(OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK)
DO UPDATE
SET 
	credit_amount = EXCLUDED.credit_amount
	,debet_amount = EXCLUDED.debet_amount;

-- Запись логов в соответствующую таблицу
INSERT INTO LOGS.DAG_LOGS
(
    dag_id
    ,info
    ,ts
)
SELECT 
    '{{dag.dag_id}}'
    ,'ODS.FT_POSTING_F upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM FT_POSTING_F_TEMP;

COMMIT TRANSACTION;