DROP DATABASE IF EXISTS LC_DB;
DROP WAREHOUSE IF EXISTS LC_WH;

CREATE WAREHOUSE LC_WH
  WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE;
CREATE DATABASE LC_DB;
CREATE SCHEMA  LC_DB.RISK;
USE WAREHOUSE LC_WH; USE DATABASE LC_DB; USE SCHEMA RISK;

CREATE OR REPLACE TABLE LC_DB.RISK.LC_SCORED (
  ID              STRING,
  PD_PREDICTION   FLOAT,
  EAD             FLOAT,
  EL              FLOAT,
  INTEREST_INCOME FLOAT,
  GRADE           STRING,
  IS_BAD          INTEGER
);

CREATE OR REPLACE FILE FORMAT LC_DB.RISK.CSV_FMT
  TYPE='CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  SKIP_HEADER=1
  NULL_IF=('','NULL','null');

CREATE OR REPLACE STAGE LC_DB.RISK.LC_STAGE
  FILE_FORMAT=LC_DB.RISK.CSV_FMT;

SHOW SCHEMAS IN DATABASE LC_DB;     -- expect RISK, PUBLIC, INFORMATION_SCHEMA
SHOW TABLES  IN SCHEMA LC_DB.RISK;  -- expect LC_SCORED
SHOW STAGES  IN SCHEMA LC_DB.RISK;  -- expect LC_STAGELC_DB.PUBLICLC_DB.RISK.LC_STAGELC_DB.RISK.LC_STAGE

ALTER STAGE LC_DB.RISK.LC_STAGE SET DIRECTORY=(ENABLE=TRUE);LC_DB.RISK.LC_STAGELC_DB.RISK.LC_STAGELC_DB.RISK.LC_STAGE

LIST @LC_DB.RISK.LC_STAGE;

COPY INTO LC_DB.RISK.LC_SCORED
FROM @LC_DB.RISK.LC_STAGE/lc_scored.csv
ON_ERROR='ABORT_STATEMENT';

SELECT COUNT(*) AS rows_loaded FROM LC_DB.RISK.LC_SCORED;
SELECT * FROM LC_DB.RISK.LC_SCORED LIMIT 10;

CREATE OR REPLACE VIEW V_PORTFOLIO_EL_SUMMARY AS
SELECT COUNT(*)                                  AS n_loans,
       SUM(EAD)                                  AS total_ead,
       SUM(EL)                                   AS total_el,
       ROUND(SUM(EL)/SUM(EAD)*100,2)             AS el_rate_pct,
       SUM(INTEREST_INCOME)                      AS total_interest_income,
       SUM(INTEREST_INCOME) - SUM(EL)            AS net_contribution,
       ROUND(AVG(PD_PREDICTION)*100,2)           AS avg_pd_pct,
       ROUND(AVG(IS_BAD)*100,2)                  AS observed_bad_rate_pct
FROM LC_DB.RISK.LC_SCORED;

-- V2. Risk + return by grade — EL rate in bps; should rank-order A→G monotonically
CREATE OR REPLACE VIEW V_GRADE_RISK_RETURN AS
SELECT GRADE,
       COUNT(*)                                     AS n_loans,
       SUM(EAD)                                      AS total_ead,
       SUM(EL)                                       AS total_el,
       ROUND(SUM(EL)/SUM(EAD)*10000,1)               AS el_rate_bps,
       SUM(INTEREST_INCOME) - SUM(EL)                AS net_contribution,
       ROUND(AVG(PD_PREDICTION)*100,2)               AS avg_pd_pct,
       ROUND(AVG(IS_BAD)*100,2)                       AS observed_bad_rate_pct
FROM LC_DB.RISK.LC_SCORED
GROUP BY GRADE ORDER BY GRADE;

-- V3. PD-band calibration — avg_pd_pct vs observed_bad_rate_pct should track per band
CREATE OR REPLACE VIEW V_PD_BAND_CALIBRATION AS
SELECT CASE WHEN PD_PREDICTION<0.05 THEN '1. <5%'
            WHEN PD_PREDICTION<0.10 THEN '2. 5-10%'
            WHEN PD_PREDICTION<0.15 THEN '3. 10-15%'
            WHEN PD_PREDICTION<0.25 THEN '4. 15-25%'
            WHEN PD_PREDICTION<0.40 THEN '5. 25-40%'
            ELSE '6. 40%+' END                       AS pd_band,
       COUNT(*)                                       AS n_loans,
       ROUND(AVG(PD_PREDICTION)*100,2)                AS avg_pd_pct,
       ROUND(AVG(IS_BAD)*100,2)                        AS observed_bad_rate_pct,
       SUM(EL)                                        AS total_el
FROM LC_DB.RISK.LC_SCORED
GROUP BY 1 ORDER BY 1;

-- V4. Approval decision at Day-8 PD cutoff (0.12)
--     Caveat: lifetime EL vs annualised income horizon mismatch — see Day 8 note.
CREATE OR REPLACE VIEW V_APPROVAL_DECISION AS
WITH s AS (
  SELECT *, CASE WHEN PD_PREDICTION<=0.12 THEN 'APPROVE' ELSE 'DECLINE' END AS decision
  FROM LC_DB.RISK.LC_SCORED
)
SELECT decision,
       COUNT(*)                                       AS n_loans,
       ROUND(COUNT(*)/SUM(COUNT(*)) OVER ()*100,1)     AS pct_of_book,
       SUM(EAD)                                        AS total_ead,
       SUM(EL)                                         AS total_el,
       SUM(INTEREST_INCOME) - SUM(EL)                  AS net_contribution,
       ROUND(SUM(EL)/SUM(EAD)*100,2)                   AS el_rate_pct,
       ROUND(AVG(IS_BAD)*100,2)                         AS observed_bad_rate_pct
FROM s
GROUP BY decision ORDER BY decision;


SELECT * FROM V_PORTFOLIO_EL_SUMMARY;
SELECT * FROM V_GRADE_RISK_RETURN;
SELECT * FROM V_PD_BAND_CALIBRATION;
SELECT * FROM V_APPROVAL_DECISION;
