BEGIN TRANSACTION;

-- Создание временной таблицы с данными, отобранными и отформатированными для вставки в DDS/ODS
CREATE TEMP TABLE MD_LEDGER_ACCOUNT_S_TEMP AS 
SELECT
    chapter::CHAR(1)
    ,chapter_name::VARCHAR
    ,section_number::INTEGER
    ,section_name::VARCHAR
    ,subsection_name::VARCHAR
    ,ledger1_account::INTEGER
    ,ledger1_account_name::VARCHAR
    ,ledger_account::INTEGER
    ,ledger_account_name::VARCHAR
    ,characteristic::CHAR(1)
    ,is_resident::BOOL
    ,is_reserve::BOOL
    ,is_reserved::BOOL
    ,is_loan::BOOL
    ,is_reserved_assets::BOOL
    ,is_overdue::BOOL
    ,is_interest::BOOL
    ,NULLIF(pair_account, 'NaN')::INT                           AS pair_account
    ,start_date::DATE
    ,end_date::DATE
    ,is_rub_only::BOOL
    ,NULLIF(min_term, 'NaN')::INT                               AS min_term
    ,NULLIF(min_term_measure, 'NaN')::VARCHAR                   AS min_term_measure
    ,NULLIF(max_term, 'NaN')::INT                               AS max_term
    ,NULLIF(max_term_measure, 'NaN')::VARCHAR                   AS max_term_measure
    ,NULLIF(ledger_acc_full_name_translit, 'NaN')::VARCHAR      AS ledger_acc_full_name_translit
    ,NULLIF(is_revaluation, 'NaN')::BOOL                        AS is_revaluation
    ,NULLIF(is_correct, 'NaN')::BOOL                            AS is_correct
FROM 
    (SELECT
        *
        ,ROW_NUMBER() OVER (PARTITION BY LEDGER_ACCOUNT, START_DATE ORDER BY id DESC) 
    FROM RAW.MD_LEDGER_ACCOUNT_S
    WHERE start_date IS NOT NULL
        AND ledger_account IS NOT NULL) c
WHERE row_number = 1;

--Вставка данных из временной таблицы в целевую таблицу слоя DDS/ODS
INSERT INTO ODS.MD_LEDGER_ACCOUNT_S
(
    chapter
    ,chapter_name
    ,section_number
    ,section_name
    ,subsection_name
    ,ledger1_account
    ,ledger1_account_name
    ,ledger_account
    ,ledger_account_name
    ,characteristic
    ,is_resident
    ,is_reserve
    ,is_reserved
    ,is_loan
    ,is_reserved_assets
    ,is_overdue
    ,is_interest
    ,pair_account
    ,start_date
    ,end_date
    ,is_rub_only
    ,min_term
    ,min_term_measure
    ,max_term
    ,max_term_measure
    ,ledger_acc_full_name_translit
    ,is_revaluation
    ,is_correct
)
SELECT * FROM MD_LEDGER_ACCOUNT_S_TEMP
ON CONFLICT(LEDGER_ACCOUNT, START_DATE)
DO UPDATE
SET 
    chapter = EXCLUDED.chapter
    ,chapter_name = EXCLUDED.chapter_name
    ,section_number = EXCLUDED.section_number
    ,section_name = EXCLUDED.section_name
    ,subsection_name = EXCLUDED.subsection_name
    ,ledger1_account = EXCLUDED.ledger1_account
    ,ledger1_account_name = EXCLUDED.ledger1_account_name
    ,ledger_account_name = EXCLUDED.ledger_account_name
    ,characteristic = EXCLUDED.characteristic
    ,is_resident = EXCLUDED.is_resident
    ,is_reserve = EXCLUDED.is_reserve
    ,is_reserved = EXCLUDED.is_reserved
    ,is_loan = EXCLUDED.is_loan
    ,is_reserved_assets = EXCLUDED.is_reserved_assets
    ,is_overdue = EXCLUDED.is_overdue
    ,is_interest = EXCLUDED.is_interest
    ,pair_account = EXCLUDED.pair_account
    ,end_date = EXCLUDED.end_date
    ,is_rub_only = EXCLUDED.is_rub_only
    ,min_term = EXCLUDED.min_term
    ,min_term_measure = EXCLUDED.min_term_measure
    ,max_term = EXCLUDED.max_term
    ,max_term_measure = EXCLUDED.max_term_measure
    ,ledger_acc_full_name_translit = EXCLUDED.ledger_acc_full_name_translit
    ,is_revaluation = EXCLUDED.is_revaluation
    ,is_correct = EXCLUDED.is_correct;

-- Запись логов в соответствующую таблицу
INSERT INTO LOGS.DAG_LOGS
(
    dag_id
    ,info
    ,ts
)
SELECT 
    '{{dag.dag_id}}'
    ,'ODS.MD_LEDGER_ACCOUNT_S upserted ' || COUNT(*)::VARCHAR || ' rows'
    ,NOW()
FROM MD_LEDGER_ACCOUNT_S_TEMP;

COMMIT TRANSACTION;