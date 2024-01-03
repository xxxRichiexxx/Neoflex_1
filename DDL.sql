CREATE SCHEMA LOGS;   

DROP TABLE IF EXISTS LOGS.DAG_LOGS;
CREATE TABLE LOGS.DAG_LOGS
(
    id SERIAL
    ,dag_id VARCHAR
    ,info VARCHAR
    ,ts TIMESTAMP DEFAULT NOW()
);

CREATE SCHEMA RAW;   ------------------------------------------------------------------

DROP TABLE IF EXISTS RAW.FT_BALANCE_F;
CREATE TABLE RAW.FT_BALANCE_F
(
    id VARCHAR
    ,on_date VARCHAR
    ,account_rk VARCHAR
    ,currency_rk VARCHAR
    ,balance_out VARCHAR
    ,ts TIMESTAMP 
);


DROP TABLE IF EXISTS RAW.FT_POSTING_F;
CREATE TABLE RAW.FT_POSTING_F
(
    id VARCHAR
	,oper_date VARCHAR
	,credit_account_rk VARCHAR
	,debet_account_rk VARCHAR
	,credit_amount VARCHAR
	,debet_amount VARCHAR
    ,ts TIMESTAMP 
);


DROP TABLE IF EXISTS RAW.MD_ACCOUNT_D;
CREATE TABLE RAW.MD_ACCOUNT_D
(
    id VARCHAR
	,data_actual_date VARCHAR
	,data_actual_end_date VARCHAR
	,account_rk VARCHAR
	,account_number VARCHAR
	,char_type VARCHAR
	,currency_rk VARCHAR
	,currency_code VARCHAR
    ,ts TIMESTAMP 
);


DROP TABLE IF EXISTS RAW.MD_CURRENCY_D;
CREATE TABLE RAW.MD_CURRENCY_D
(
    id VARCHAR
    ,currency_rk VARCHAR
    ,data_actual_date VARCHAR
    ,data_actual_end_date VARCHAR
    ,currency_code VARCHAR
    ,code_iso_char VARCHAR
    ,ts TIMESTAMP 
);


DROP TABLE IF EXISTS RAW.MD_EXCHANGE_RATE_D;
CREATE TABLE RAW.MD_EXCHANGE_RATE_D
(
    id VARCHAR
    ,data_actual_date VARCHAR
    ,data_actual_end_date VARCHAR
    ,currency_rk VARCHAR
    ,reduced_cource VARCHAR
    ,code_iso_num VARCHAR
    ,ts TIMESTAMP 
);


DROP TABLE IF EXISTS RAW.MD_LEDGER_ACCOUNT_S;
CREATE TABLE RAW.MD_LEDGER_ACCOUNT_S
(
    id VARCHAR
    ,chapter VARCHAR
    ,chapter_name VARCHAR
    ,section_number VARCHAR
    ,section_name VARCHAR
    ,subsection_name VARCHAR
    ,ledger1_account VARCHAR
    ,ledger1_account_name VARCHAR
    ,ledger_account VARCHAR
    ,ledger_account_name VARCHAR
    ,characteristic VARCHAR
    ,is_resident VARCHAR
    ,is_reserve VARCHAR
    ,is_reserved VARCHAR
    ,is_loan VARCHAR
    ,is_reserved_assets VARCHAR
    ,is_overdue VARCHAR
    ,is_interest VARCHAR
    ,pair_account VARCHAR
    ,start_date VARCHAR
    ,end_date VARCHAR
    ,is_rub_only VARCHAR
    ,min_term VARCHAR
    ,min_term_measure VARCHAR
    ,max_term VARCHAR
    ,max_term_measure VARCHAR
    ,ledger_acc_full_name_translit VARCHAR
    ,is_revaluation VARCHAR
    ,is_correct VARCHAR
    ,ts TIMESTAMP 
);

CREATE SCHEMA ODS;    -------------------------------------------------------------------

DROP TABLE IF EXISTS ODS.FT_BALANCE_F;
DROP TABLE IF EXISTS ODS.FT_POSTING_F;
DROP TABLE IF EXISTS ODS.MD_ACCOUNT_D;
DROP TABLE IF EXISTS ODS.MD_LEDGER_ACCOUNT_S;
DROP TABLE IF EXISTS ODS.MD_EXCHANGE_RATE_D;
DROP TABLE IF EXISTS ODS.MD_CURRENCY_D;


CREATE TABLE ODS.MD_CURRENCY_D
(
    currency_rk INT 
    ,data_actual_date DATE 
    ,data_actual_end_date DATE
    ,currency_code INT
    ,code_iso_char CHAR(6)

    ,PRIMARY KEY(currency_rk, data_actual_date)
);

COMMENT ON TABLE ODS.MD_CURRENCY_D IS 'справочник валют';


CREATE TABLE ODS.MD_EXCHANGE_RATE_D
(
    data_actual_date DATE
    ,data_actual_end_date DATE
    ,currency_rk INT
    ,reduced_cource NUMERIC(8,3)
    ,code_iso_num INT

    ,PRIMARY KEY(DATA_ACTUAL_DATE, CURRENCY_RK)
);

COMMENT ON TABLE ODS.MD_CURRENCY_D IS 'курсы валют';


CREATE TABLE ODS.MD_LEDGER_ACCOUNT_S
(
    chapter CHAR(1)
    ,chapter_name VARCHAR
    ,section_number INTEGER
    ,section_name VARCHAR
    ,subsection_name VARCHAR
    ,ledger1_account INTEGER
    ,ledger1_account_name VARCHAR
    ,ledger_account INTEGER NOT NULL
    ,ledger_account_name VARCHAR
    ,characteristic CHAR(1)
    ,is_resident BOOL
    ,is_reserve BOOL
    ,is_reserved BOOL
    ,is_loan BOOL
    ,is_reserved_assets BOOL
    ,is_overdue BOOL
    ,is_interest BOOL
    ,pair_account VARCHAR
    ,start_date DATE NOT NULL
    ,end_date DATE
    ,is_rub_only BOOL
    ,min_term INT
    ,min_term_measure VARCHAR
    ,max_term INT
    ,max_term_measure VARCHAR
    ,ledger_acc_full_name_translit VARCHAR
    ,is_revaluation BOOL
    ,is_correct BOOL

    ,PRIMARY KEY(LEDGER_ACCOUNT, START_DATE)
);

COMMENT ON TABLE ODS.MD_LEDGER_ACCOUNT_S IS 'справочник балансовых счётов';

CREATE TABLE ODS.MD_ACCOUNT_D
(
	data_actual_date DATE
	,data_actual_end_date DATE NOT NULL
    ,account_rk INT
	,account_number NUMERIC(40)
	,char_type CHAR(1) NOT NULL
	,currency_rk INT NOT NULL
	,currency_code INT NOT NULL

    ,PRIMARY KEY(DATA_ACTUAL_DATE, ACCOUNT_RK)
);

COMMENT ON TABLE ODS.MD_ACCOUNT_D IS 'информация о счетах клиентов';


CREATE TABLE ODS.FT_POSTING_F
(   
	oper_date DATE
	,credit_account_rk INT
	,debet_account_rk INT
	,credit_amount NUMERIC(9,3) NOT NULL
	,debet_amount NUMERIC(9,3) NOT NULL

    ,PRIMARY KEY(OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK) 
);

COMMENT ON TABLE ODS.MD_ACCOUNT_D IS 'проводки (движения средств) по счетам';


CREATE TABLE ODS.FT_BALANCE_F
(
    on_date DATE
    ,account_rk INT
    ,currency_rk INT
    ,balance_out NUMERIC(9,3) NOT NULL

    ,PRIMARY KEY(ON_DATE, ACCOUNT_RK)
);

COMMENT ON TABLE ODS.MD_ACCOUNT_D IS 'остатки средств на счетах';

CREATE SCHEMA DDS;    -------------------------------------------------------------------

DROP TABLE IF EXISTS DDS.FT_BALANCE_F;
DROP TABLE IF EXISTS DDS.FT_POSTING_F;
DROP TABLE IF EXISTS DDS.MD_ACCOUNT_D;
DROP TABLE IF EXISTS DDS.MD_LEDGER_ACCOUNT_S;
DROP TABLE IF EXISTS DDS.MD_EXCHANGE_RATE_D;
DROP TABLE IF EXISTS DDS.MD_CURRENCY_D;


CREATE TABLE DDS.MD_CURRENCY_D
(
    id SERIAL PRIMARY KEY
    ,currency_rk INT NOT NULL
    ,data_actual_date DATE NOT NULL
    ,data_actual_end_date DATE
    ,currency_code INT
    ,code_iso_char CHAR(6)
    ,ts TIMESTAMP DEFAULT NOW()

    ,UNIQUE(currency_rk, data_actual_date)
);

COMMENT ON TABLE DDS.MD_CURRENCY_D IS 'справочник валют';


CREATE TABLE DDS.MD_EXCHANGE_RATE_D
(
    id SERIAL PRIMARY KEY
    ,data_actual_date DATE
    ,data_actual_end_date DATE
    ,currency_rk INT NOT NULL
    ,currency_id INT NOT NULL REFERENCES DDS.MD_CURRENCY_D(id)
    ,reduced_cource NUMERIC(8,3)
    ,code_iso_num INT
    ,ts TIMESTAMP DEFAULT NOW()

    ,UNIQUE(DATA_ACTUAL_DATE, CURRENCY_RK)
);

COMMENT ON TABLE DDS.MD_CURRENCY_D IS 'курсы валют';


CREATE TABLE DDS.MD_LEDGER_ACCOUNT_S
(
    id SERIAL PRIMARY KEY
    ,chapter CHAR(1)
    ,chapter_name VARCHAR
    ,section_number INTEGER
    ,section_name VARCHAR
    ,subsection_name VARCHAR
    ,ledger1_account INTEGER
    ,ledger1_account_name VARCHAR
    ,ledger_account INTEGER NOT NULL
    ,ledger_account_name VARCHAR
    ,characteristic CHAR(1)
    ,is_resident BOOL
    ,is_reserve BOOL
    ,is_reserved BOOL
    ,is_loan BOOL
    ,is_reserved_assets BOOL
    ,is_overdue BOOL
    ,is_interest BOOL
    ,pair_account VARCHAR
    ,start_date DATE NOT NULL
    ,end_date DATE
    ,is_rub_only BOOL
    ,min_term INT
    ,min_term_measure VARCHAR
    ,max_term INT
    ,max_term_measure VARCHAR
    ,ledger_acc_full_name_translit VARCHAR
    ,is_revaluation BOOL
    ,is_correct BOOL
    ,ts TIMESTAMP DEFAULT NOW()

    ,UNIQUE(LEDGER_ACCOUNT, START_DATE)
);

COMMENT ON TABLE DDS.MD_LEDGER_ACCOUNT_S IS 'справочник балансовых счётов';

CREATE TABLE DDS.MD_ACCOUNT_D
(
    id SERIAL PRIMARY KEY
	,data_actual_date DATE NOT NULL
	,data_actual_end_date DATE NOT NULL
    ,account_rk INT NOT NULL
	,account_number NUMERIC(40)
    ,ledger_account_id INT REFERENCES DDS.MD_LEDGER_ACCOUNT_S(id)
	,char_type CHAR(1) NOT NULL
    ,currency_id INT NOT NULL REFERENCES DDS.MD_CURRENCY_D(id)
	,currency_rk INT NOT NULL
	,currency_code INT NOT NULL
    ,ts TIMESTAMP DEFAULT NOW()

    ,UNIQUE(DATA_ACTUAL_DATE, ACCOUNT_RK)
);

COMMENT ON TABLE DDS.MD_ACCOUNT_D IS 'информация о счетах клиентов';


CREATE TABLE DDS.FT_POSTING_F
(   
    id SERIAL PRIMARY KEY
	,oper_date DATE NOT NULL
    ,credit_account_id INT NOT NULL REFERENCES DDS.MD_ACCOUNT_D(id)
	,credit_account_rk INT NOT NULL
    ,debet_account_id INT NOT NULL REFERENCES DDS.MD_ACCOUNT_D(id)
	,debet_account_rk INT NOT NULL
	,credit_amount NUMERIC(9,3) NOT NULL
	,debet_amount NUMERIC(9,3) NOT NULL
    ,ts TIMESTAMP DEFAULT NOW()

    ,UNIQUE(OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK) 
);

COMMENT ON TABLE DDS.MD_ACCOUNT_D IS 'проводки (движения средств) по счетам';


CREATE TABLE DDS.FT_BALANCE_F
(
    id SERIAL PRIMARY KEY
    ,on_date DATE NOT NULL
    ,account_id INT NOT NULL REFERENCES DDS.MD_ACCOUNT_D(id)
    ,account_rk INT NOT NULL
    ,currency_id INT NOT NULL REFERENCES DDS.MD_CURRENCY_D(id)
    ,currency_rk INT
    ,balance_out NUMERIC(9,3) NOT NULL
    ,ts TIMESTAMP DEFAULT NOW()

    ,UNIQUE(ON_DATE, ACCOUNT_RK)
);

COMMENT ON TABLE DDS.MD_ACCOUNT_D IS 'остатки средств на счетах';

------------------------------------------------------------

CREATE SCHEMA REJECTED_DATA;

------------------------------------------------------------

CREATE ROLE data_analyst;
GRANT USAGE ON SCHEMA dds TO data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA DDS TO data_analyst;

CREATE USER analyst1
PASSWORD 'test';

GRANT data_analyst TO analyst1;


CREATE ROLE data_engineer;
GRANT USAGE ON SCHEMA raw, dds, logs, rejected_data TO data_engineer;
GRANT ALL PRIVILEGES ON SCHEMA raw, dds, logs, rejected_data TO data_engineer;

CREATE USER engineer1
PASSWORD 'test';

GRANT data_engineer TO engineer1;