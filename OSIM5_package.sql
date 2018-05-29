
--================================================================================
-- POSTGRES PACKAGE OSIM5 v2.1.000
-- Person Condition Drug Simulator
--
--
--================================================================================
--    Postgres Version -
--
--    Georgia Tech Research Institute
--    Any scientific publication that is based on this work should include a
--    reference to ____________________
--
--    Original Version -
--
--    Observational Medical Outcomes Partnership
--    06 January 2011
--
--    Oracle PL/SQL Package for analysis and simulation of patient data
--
--    ?2011 Foundation for the National Institutes of Health (FNIH)
--
--    Licensed under the Apache License, Version 2.0 (the "License"); you may not
--    use this file except in compliance with the License. You may obtain a copy
--    of the License at http://omop.fnih.org/publiclicense.
--
--    Unless required by applicable law or agreed to in writing, software
--    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
--    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Any
--    redistributions of this work or any derivative work or modification based on
--    this work should be accompanied by the following source attribution: "This
--    work is based on work by the Observational Medical Outcomes Partnership
--    (OMOP) and used under license from the FNIH at
--    http://omop.fnih.org/publiclicense.
--
--    Any scientific publication that is based on this work should include a
--    reference to http://omop.fnih.org.
--
--================================================================================
--
-- DESCRIPTION
--
-- This package contains methods to analyze an existing CDM format database to
-- populate a series of stratified probability and transition tables.
--
-- The package also includes methods to use the probability and transition tables
-- to simulate data with similar characteristics of the analyzed CDM database.
--
-- Additionally, optional methods are included to adjust prevalence levels for
-- known outcomes and preventative drug therapies.
--
-- A series of bucketing functions may be modified to adjust stratifaction
-- ranges for age, condition, and drug counts.  However, the bucketing functions
-- used during the analysis phase must match the bucket functions used during
-- the simulation.
--
--================================================================================
--
-- CHANGE LOG
--  v0.1.000 - 17 May 2010 - R Murray (ProSanos) - Proof of Concept
--  v1.1.000 - 15 Jul 2010 - R Murray (ProSanos) - Condition Simulation
--  v1.2.000 - 27 Sep 2010 - R Murray (ProSanos) - Drug Simulation
--  v1.3.000 - 01 Oct 2010 - R Murray (ProSanos) - Drug Outcome Simulation
--  v1.4.000 - 22 Oct 2010 - R Murray (ProSanos) - Modified subsequent drug era logic
--                                                 to distribute drawn total exposure across
--                                                 drawn count eras over drawn duration
--  v1.5.000 - 10 Nov 2010 - R Murray (ProSanos) - Additional parameters and procedures
--                                                 for parallelization
--  v1.5.001 - 08 Dec 2010 - R Murray (ProSanos) - Added condition and drug era type
--                                                 to analysis and simulation
--  v1.5.002 - 06 Jan 2011 - R Murray (UBC)      - Fixed potential division by zero in outcomes
--  v1.5.003 - 02 Feb 2011 - R Murray (UBC)      - Code reorganization
--  V1.5.004 - 05 Feb 2011 - R Murray (UBC)      - Fixed Year of Birth error
--  V1.5.005 - 15 Feb 2011 - R Murray (UBC)      - Modified min and max db date to only
--                                                 use observation period dates
--  V2.1.000 - 27 Mar 2018 - K Mukadam (GTRI)    - Modified code for OMOP CDM v5 and converted to PostgreSQL

--================================================================================
-- TODO

-- Outcome/risk function update
-- Fix logging (issue due to PostgreSQL i.e. no autonomous transactions)
--================================================================================


SET SEARCH_PATH TO synthetic_data_generation_test, public;
CREATE EXTENSION IF NOT EXISTS tablefunc; -- for norm_random function

DROP TYPE IF EXISTS COND_TRANSITION CASCADE;
CREATE TYPE COND_TRANSITION AS (
    gender_concept_id      INTEGER,
    age_range              INTEGER,
    cond_count_bucket      INTEGER,
    time_remaining         INTEGER,
    condition1_concept_id  INTEGER,
    condition2_concept_id  INTEGER,
    delta_days             FLOAT );

DROP TYPE IF EXISTS DRUG_COND_OUTCOME CASCADE;
CREATE TYPE DRUG_COND_OUTCOME AS (
  person_id        INTEGER,
  drug_era_id      INTEGER,
  condition_era_id INTEGER);

/*=========================================================================
  | FUNCTION CONDITION_COUNT_BUCKET
  |
  | Returns Condition Count Bucket (maximum condition count value) for conditon count.
  | This function can be altered to change stratification.
  |
  |==========================================================================
  */
CREATE OR REPLACE FUNCTION OSIM__condition_count_bucket(BIGINT)
RETURNS INTEGER AS $$
DECLARE condition_count ALIAS FOR $1;
  BEGIN
    CASE TRUE
      WHEN condition_count <= 2 THEN RETURN 2;
      WHEN condition_count <= 7 THEN RETURN 7;
      WHEN condition_count <= 25 THEN RETURN 25;
      ELSE RETURN 2000;
    END CASE;
  END;
  $$ LANGUAGE 'plpgsql';

/*=========================================================================
  | FUNCTION DRUG_COUNT_BUCKET
  |
  | Returns Drug Count Bucket (maximum condition count value) for drug count.
  | This function can be altered to change stratification.
  |
  |==========================================================================
  */
CREATE OR REPLACE FUNCTION OSIM__drug_count_bucket(BIGINT)
  RETURNS INTEGER AS $$
  DECLARE drug_count ALIAS FOR $1;
  BEGIN
    CASE TRUE
      WHEN drug_count <= 2 THEN RETURN 2;
      WHEN drug_count <= 7 THEN RETURN 7;
      WHEN drug_count <= 25 THEN RETURN 25;
      ELSE RETURN 2000;
    END CASE;
  END;
$$ LANGUAGE 'plpgsql';

/*=========================================================================
  | FUNCTION PROCEDURE_COUNT_BUCKET
  |
  | Returns Procedure Count Bucket (maximum condition count value) for procedure count.
  | This function can be altered to change stratification.
  |
  |==========================================================================
  */
CREATE OR REPLACE FUNCTION OSIM__procedure_count_bucket(BIGINT)
  RETURNS INTEGER AS $$
  DECLARE procedure_count ALIAS FOR $1;
  BEGIN
    CASE TRUE
      WHEN procedure_count <= 2 THEN RETURN 2;
      WHEN procedure_count <= 7 THEN RETURN 7;
      WHEN procedure_count <= 25 THEN RETURN 25;
      ELSE RETURN 2000;
    END CASE;
  END;
$$ LANGUAGE 'plpgsql';

/*=========================================================================
  | FUNCTION AGE_BUCKET
  |
  | Returns Age Bucket (maximum age value) for age.
  | This function can be altered to change stratification.
  |
  |==========================================================================
  */
CREATE OR REPLACE FUNCTION OSIM__age_bucket (NUMERIC)
RETURNS INTEGER AS $$
  DECLARE age ALIAS FOR $1;
  BEGIN
    CASE TRUE
      WHEN age IS NULL THEN RETURN NULL;
      WHEN age < 6 THEN RETURN 6;
      WHEN age < 14 THEN RETURN 14;
      WHEN age < 20 THEN RETURN 20;
      WHEN age < 55 THEN RETURN 55;
      WHEN age < 70 THEN RETURN 70;
      ELSE RETURN 120;
    END CASE;
  END;
$$ LANGUAGE 'plpgsql';

  /*=========================================================================
  | FUNCTION time_observed_bucket
  |
  | Function to create time bucket from days reaming
  |
  | Currently full semi years remaining
  |
  |==========================================================================
  */
  CREATE OR REPLACE FUNCTION OSIM__time_observed_bucket (INTEGER)
  RETURNS INTEGER AS $$
  DECLARE days ALIAS FOR $1;
  BEGIN
    CASE TRUE
      WHEN days > 0 THEN RETURN FLOOR((1+days) / 182.625);
      ELSE RETURN 0;
    END CASE;
  END;
$$ LANGUAGE 'plpgsql';

/*=========================================================================
  | CREATE FUNCTION ROUND_DAYS
  |
  | Rounds days > 75 to 30 day increments (90,120,150,..)
  |
  |==========================================================================
*/
  CREATE OR REPLACE FUNCTION OSIM__round_days(INTEGER)
  RETURNS INTEGER AS $$
  DECLARE days ALIAS FOR $1;
  BEGIN
    CASE
      WHEN days <= 75 THEN RETURN ROUND(days);
      ELSE RETURN ROUND(days/30) * 30;
    END CASE;
  END;
  $$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION OSIM__round_days(BIGINT)
  RETURNS INTEGER AS $$
  DECLARE days ALIAS FOR $1;
  BEGIN
    CASE
      WHEN days <= 75 THEN RETURN ROUND(days);
      ELSE RETURN ROUND(days/30) * 30;
    END CASE;
  END;
  $$ LANGUAGE 'plpgsql';

/*=========================================================================
  | CREATE FUNCTION RANDOMIZE_DAYS
  |
  | Adds +- 15 to rounded 30 day values > 45
  |
  |==========================================================================
*/
  CREATE OR REPLACE FUNCTION OSIM__randomize_days(INTEGER)
  RETURNS INTEGER AS $$
  DECLARE days ALIAS FOR $1;
  BEGIN
    CASE TRUE
      WHEN days <= 75 THEN RETURN ROUND(days);
      ELSE RETURN ROUND(days - 15 + random() * 30);
    END CASE;
  END;
  $$ LANGUAGE 'plpgsql';

/*=========================================================================
  | CREATE FUNCTION DURATION_DAYS_BUCKET
  |
  | Rounds days > 6 to 30 day increments (90,120,150,..)
  |
  |==========================================================================
  */
  CREATE OR REPLACE FUNCTION OSIM__duration_days_bucket(INTEGER)
  RETURNS INTEGER AS $$
  DECLARE
    days ALIAS FOR $1;
  BEGIN
    CASE TRUE
      WHEN days <= 7 THEN RETURN 7;
      ELSE RETURN 8;
    END CASE;
  END;
  $$ LANGUAGE 'plpgsql';


  /*=========================================================================
  | CREATE FUNCTION min_num
  |
  | Returns the lesser of two INTEGERs
  |
  |==========================================================================
  */
  CREATE OR REPLACE FUNCTION OSIM__min_num (INTEGER, INTEGER)
  RETURNS INTEGER AS $$
  DECLARE
    value1 ALIAS FOR $1;
    value2 ALIAS FOR $2;
  BEGIN
    RETURN CASE WHEN value1 <= value2 THEN value1 ELSE value2 END;
  END;
  $$ LANGUAGE 'plpgsql';

/*=========================================================================
  | FUNCTION get_first_cond_transitions
  |
  | retrieves every first condition era transition
  |
  | This is slower as a function than as a view, but allows it to be part of
  | the OSIM package and eliminates a circular dependency that makes
  | compiling difficult.
  |
  |==========================================================================
  */

CREATE OR REPLACE FUNCTION OSIM__get_first_cond_transitions()
  RETURNS SETOF COND_TRANSITION AS $$
    DECLARE
      rs COND_TRANSITION;
    BEGIN
      for rs in SELECT
        strata.gender_concept_id,
        coalesce(osim__age_bucket(LAG(strata.age,1)
          OVER (PARTITION BY strata.person_id
          ORDER BY condition_era_start_date, condition_concept_id)),strata.age)
            AS age_range,
        OSIM__condition_count_bucket(strata.condition_concepts) AS cond_count_bucket,
        OSIM__time_observed_bucket(strata.observation_period_end_date -
          coalesce(LAG(cond.condition_era_start_date,1)
            OVER (PARTITION BY strata.person_id
            ORDER BY condition_era_start_date, condition_concept_id),
              strata.observation_period_start_date)) AS time_remaining,
        coalesce(LAG(condition_concept_id,1)
          OVER (PARTITION BY strata.person_id
          ORDER BY condition_era_start_date, condition_concept_id),-1)
            AS condition1_concept_id,
        condition_concept_id AS condition2_concept_id,
        OSIM__ROUND_DAYS(cond.condition_era_start_date -
          coalesce(LAG(cond.condition_era_start_date,1)
            OVER (PARTITION BY strata.person_id
            ORDER BY condition_era_start_date, condition_concept_id),
              strata.observation_period_start_date)) AS delta_days
      FROM v_src_person_strata strata
      INNER JOIN v_src_first_conditions cond
        ON strata.person_id = cond.person_id
      ORDER BY 1,2,3,4
    LOOP
      RETURN NEXT rs;
    END LOOP;
    RETURN;
  END;
$$ LANGUAGE 'plpgsql';

/*=========================================================================
  | FUNCTION get_outcome_drug_eras
  |
  | retrieves drug eras with outcomes matching outcome definition
  |
  | This is slower as a function than as a view, but allows it to be part of
  | the OSIM package and eliminates a circular dependency that makes
  | compiling difficult.
  |
  |==========================================================================
  */
CREATE OR REPLACE FUNCTION OSIM__get_outcome_drug_eras (INTEGER, INTEGER, VARCHAR, INTEGER, INTEGER)
  RETURNS SETOF osim_drug_era AS $$
  DECLARE
  drug_concept_id ALIAS FOR $1;
  condition_concept_id ALIAS FOR $2;
  outcome_risk_type  ALIAS FOR $3;
  outcome_onset_days_min ALIAS FOR $4;
  outcome_onset_days_max ALIAS FOR $5;
  p_drug_concept_id      INTEGER;
  p_condition_concept_id INTEGER;
  outcomes_cur CURSOR FOR
      SELECT DISTINCT drug.*
      FROM osim_drug_era drug
      INNER JOIN
       (SELECT DISTINCT
          person_id,
          FIRST_VALUE(drug_era_id)
            OVER (PARTITION BY person_id
                  ORDER BY drug_era_start_date) AS drug_era_id,
          FIRST_VALUE(drug_era_start_date)
            OVER (PARTITION BY person_id
                  ORDER BY drug_era_start_date) AS drug_era_start_date
        FROM osim_drug_era
        WHERE drug_concept_id = p_drug_concept_id) first_drug
        ON drug.person_id = first_drug.person_id
      INNER JOIN osim_condition_era cond ON drug.person_id = cond.person_id
        AND cond.condition_concept_id = p_condition_concept_id
      WHERE drug.drug_concept_id = p_drug_concept_id
        AND 1 =
          CASE
            WHEN outcome_risk_type = 'first exposure' THEN
              CASE
                WHEN drug.drug_era_start_date = first_drug.drug_era_start_date THEN 1
                ELSE 0
              END
            ELSE 1
          END
        AND 1 =
          CASE
            WHEN (outcome_risk_type = 'insidious'
              OR outcome_risk_type = 'accumulative') THEN
              CASE
                WHEN cond.condition_era_start_date
                  BETWEEN drug.drug_era_start_date AND drug.drug_era_end_date THEN 1
                ELSE 0
              END
            WHEN cond.condition_era_start_date
              BETWEEN drug.drug_era_start_date + coalesce(outcome_onset_days_min ,0)
                AND drug.drug_era_start_date
                 + coalesce(outcome_onset_days_max, drug.drug_era_end_date
                    - drug.drug_era_start_date) THEN 1
            ELSE 0
          END;
  BEGIN
    p_drug_concept_id := drug_concept_id;
    p_condition_concept_id := condition_concept_id;
    FOR rs IN outcomes_cur LOOP
      RETURN NEXT ROW(rs);
    END LOOP;
    RETURN;
  END;
  $$ LANGUAGE plpgsql;

 /*=========================================================================
  | FUNCTION get_outcome_eras
  |
  | retrieves person_id, drug_era_id, and condition_era_ids for outcomes
  | matching the definition
  |
  | This is slower as a function than as a view, but allows it to be part of
  | the OSIM package and eliminates a circular dependency that makes
  | compiling difficult.
  |
  |==========================================================================
  */
CREATE OR REPLACE FUNCTION OSIM__get_outcome_eras (INTEGER, INTEGER, VARCHAR, INTEGER, INTEGER)
RETURNS SETOF DRUG_COND_OUTCOME As $$
 DECLARE
  drug_concept_id ALIAS FOR $1;
  condition_concept_id ALIAS FOR $2;
  outcome_risk_type  ALIAS FOR $3;
  outcome_onset_days_min ALIAS FOR $4;
  outcome_onset_days_max ALIAS FOR $5;
  p_drug_concept_id      INTEGER;
  p_condition_concept_id INTEGER;
  outcomes_cur CURSOR  FOR
      SELECT DISTINCT
        drug.person_id,
        drug.drug_era_id,
        cond.condition_era_id
      FROM osim_drug_era drug
      INNER JOIN
       (SELECT DISTINCT
          person_id,
          FIRST_VALUE(drug_era_id)
            OVER (PARTITION BY person_id
                  ORDER BY drug_era_start_date) AS drug_era_id,
          FIRST_VALUE(drug_era_start_date)
            OVER (PARTITION BY person_id
                  ORDER BY drug_era_start_date) AS drug_era_start_date
        FROM osim_drug_era
        WHERE drug_concept_id = p_drug_concept_id) first_drug
        ON drug.person_id = first_drug.person_id
      INNER JOIN osim_condition_era cond ON drug.person_id = cond.person_id
        AND cond.condition_concept_id = p_condition_concept_id
      WHERE drug.drug_concept_id = p_drug_concept_id
        AND 1 =
          CASE
            WHEN outcome_risk_type = 'first exposure' THEN
              CASE
                WHEN drug.drug_era_start_date = first_drug.drug_era_start_date THEN 1
                ELSE 0
              END
            ELSE 1
          END
        AND 1 =
          CASE
            WHEN (outcome_risk_type = 'insidious'
              OR outcome_risk_type = 'accumulative') THEN
              CASE
                WHEN cond.condition_era_start_date
                  BETWEEN drug.drug_era_start_date AND drug.drug_era_end_date THEN 1
                ELSE 0
              END
            WHEN cond.condition_era_start_date
              BETWEEN drug.drug_era_start_date + coalesce(outcome_onset_days_min ,0)
                AND drug.drug_era_start_date
                 + coalesce(outcome_onset_days_max, drug.drug_era_end_date
                    - drug.drug_era_start_date) THEN 1
            ELSE 0
          END;
  BEGIN
    p_drug_concept_id := drug_concept_id;
    p_condition_concept_id := condition_concept_id;
    FOR rs IN outcomes_cur LOOP
      RETURN NEXT ROW(rs);
    END LOOP;
   RETURN;

  END;
$$ LANGUAGE plpgsql;

   /*=========================================================================
  | CREATE OR REPLACE FUNCTION INSERT_LOG
  |
  | Insert timestamped message into process log and also performs
  | DBMS_OUTPUT.PUT_LINE of the message.
  |
  | This is an autonomous_transacton with its own nested commit,
  | so the run log is actively updated and can be viewed while the simulator
  | is running.

  | Update - Since Postgres does not support commit statements or autonomous transactions,
  this does not work as expected
  |==========================================================================
  */

  CREATE OR REPLACE FUNCTION insert_log(text, text)
    RETURNS VOID AS $$
  DECLARE
    m ALIAS FOR $1;
    procedure_name ALIAS FOR $2;
  BEGIN
    INSERT INTO osim_log (stored_procedure_name, message)
    VALUES (procedure_name, m);
  END;
  $$ LANGUAGE plpgsql;

--   CREATE OR REPLACE FUNCTION insert_log_atx(text, text)
--     RETURNS VOID AS $$
--   DECLARE
--     m ALIAS FOR $1;
--     procedure_name ALIAS FOR $2;
--     open_connections TEXT [] := PUBLIC.dblink_get_connections();
--     --v_conn_str  text := 'port=5432 dbname=postgres host=ohdsi_v5.i3l.gatech.edu user=ohdsi_admin_user password=J4ckets_4_synpUf';
--     v_conn_str  text := 'port=5432 dbname=postgres host=localhost user=kausarm password=Pass@123';
--     v_query     text;
--     my_dblink_name TEXT := 'logging_dblink' ;
--   BEGIN
--     IF open_connections IS NULL
--        OR NOT open_connections @> ARRAY [ my_dblink_name ] THEN
--         PERFORM PUBLIC.dblink_connect(
--             my_dblink_name, v_conn_str);
--         raise debug 'New db link connection made' ;
--     ELSE
--         raise debug 'Db link connection exists, re-using' ;
--     END IF;
--
--     v_query := 'SELECT true FROM insert_log_atx( ' || quote_nullable(m) ||
-- 		 ',' || quote_nullable(procedure_name) || ')';
--     PERFORM PUBLIC.dblink_exec(my_dblink_name, v_query);
--
--     -- PERFORM * FROM dblink(v_conn_str, v_query) AS p (ret boolean);
--   END;
--   $$ LANGUAGE plpgsql;
/*=========================================================================
  | CREATE OR REPLACE FUNCTION INS_SRC_DB_ATTRIBUTES
  |
  | Analyze source CDM database and store results for:
  |   min_db_date
  |   max_db_date
  |   condition_occurrence_type
  |   drug_exposure_type
  |   persons count
  |   condition_eras count
  |
  |==========================================================================
  */
CREATE OR REPLACE FUNCTION ins_src_db_attributes()
RETURNS VOID AS $$
  DECLARE
    db_min_date           DATE;
    db_max_date           DATE;
    persons_count         INTEGER;
    condition_eras_count  INTEGER;
    drug_eras_count       INTEGER;
    procedure_occurrences_count INTEGER;
--     db_cond_era_type_code VARCHAR(3);
--     db_drug_era_type_code VARCHAR(3);
    num_rows              INTEGER;
    MESSAGE               text;
  BEGIN

    PERFORM insert_log('Getting general Source Database Counts', 'ins_src_db_attributes');
    SELECT MIN(min_db_date)
    INTO db_min_date
    FROM
     (SELECT MIN(observation_period_start_date) AS min_db_date
      FROM v_src_observation_period) t1;

    SELECT MAX(max_db_date)
    INTO db_max_date
    FROM
     (SELECT MAX(observation_period_end_date) AS max_db_date
      FROM v_src_observation_period) t2;

    SELECT COUNT(DISTINCT person.person_id)
    INTO persons_count
    FROM v_src_person person;

    SELECT COUNT(DISTINCT cond.condition_occurrence_id)
    INTO condition_eras_count
    FROM v_src_condition_era1_ids cond;

    SELECT COUNT(DISTINCT drug.drug_exposure_id)
    INTO drug_eras_count
    FROM v_src_drug_era1_ids drug;

    SELECT COUNT(DISTINCT procedure.procedure_occurrence_id)
    INTO procedure_occurrences_count
    FROM v_src_procedure_occurrence1_ids procedure;

    MESSAGE := 'Source CDM Attributes:'
      || ' db_min_date=' || db_min_date
      || ', db_max_date=' || db_max_date
      || ', persons_count=' || persons_count
      || ', condition_eras_count=' || condition_eras_count
      || ', drug_eras_count=' || drug_eras_count;
--     insert_log(MESSAGE, 'ins_src_db_attributes');

    TRUNCATE TABLE osim_src_db_attributes;

    INSERT INTO osim_src_db_attributes
      (db_min_date, db_max_date, persons_count, condition_eras_count,
       drug_eras_count, procedure_occurrences_count)
    SELECT db_min_date, db_max_date, persons_count, condition_eras_count,
      drug_eras_count, procedure_occurrences_count;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_src_db_attributes.';
    --COMMIT;
    --PERFORM pg_background_launch('SELECT insert_log(MESSAGE, ''ins_src_db_attributes'');');
    PERFORM insert_log(MESSAGE, 'ins_src_db_attributes');
    --PERFORM pg_background_launch('SELECT insert_log(''Processing complete'', ''ins_src_db_attributes'');');
    PERFORM insert_log('Processing complete', 'ins_src_db_attributes');
    raise notice 'Processing complete ins_src_db_attributes, rows = %', num_rows;

  EXCEPTION
    WHEN OTHERS THEN
      raise notice '% %', SQLERRM, SQLSTATE;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_gender_probability()
  /*=========================================================================
  | PROCEDURE INS_GENDER_PROBABILITY
  |
  | Pr(Gender)
  |
  | Analyze source CDM database and store results for:
  |   Gender Probability
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows             INTEGER;
    MESSAGE              TEXT;
  BEGIN
    PERFORM insert_log('Starting Gender Probability Analysis', 'ins_gender_probability');

    TRUNCATE TABLE osim_gender_probability;
    --COMMIT;
    INSERT /*+ append nologging */ INTO osim_gender_probability
      (gender_concept_id, n, accumulated_probability)
      SELECT
        gender_concept_id,
        n,
        SUM(probability)
            OVER
              (ORDER BY probability DESC
                ROWS UNBOUNDED PRECEDING) accumulated_probability
      FROM
       (SELECT DISTINCT
          gender_concept_id,
          COUNT(*) AS n,
          1.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER(),0) AS probability

        FROM v_src_person_strata strata
        GROUP BY gender_concept_id) t1;
    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_gender_probability.';
    PERFORM insert_log(MESSAGE, 'ins_gender_probability');
    --COMMIT;
    -- Ensure last accumulated_probability = 1.0
    UPDATE osim_gender_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (ORDER BY accumulated_probability DESC)
      FROM osim_gender_probability);
    --COMMIT;
    PERFORM insert_log('Processing complete', 'ins_gender_probability');
    raise notice 'Processing complete ins_gender_probability, rows = %', num_rows;

  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_gender_probability');
    raise notice '% %', SQLERRM, SQLSTATE;

  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ins_age_at_obs_probability()
  /*=========================================================================
  | PROCEDURE INS_AGE_AT_OBS_PROBABILITY
  |
  | Pr(Age | Gender)
  |
  | Analyze source CDM database and store results for:
  |   Age Probability at beginning of observation period
  |   Based on Gender
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows             INTEGER;
    MESSAGE              text;
  BEGIN

    PERFORM insert_log('Starting Age Probability Analysis', 'ins_age_at_obs_probability');

    TRUNCATE TABLE osim_age_at_obs_probability;
    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_age_at_obs_probability
      (gender_concept_id, age_at_obs, n, accumulated_probability)
      SELECT
        gender_concept_id,
        age_at_obs,
        n,
        SUM(probability)
          OVER
           (PARTITION BY gender_concept_id
            ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
      FROM
       (SELECT
          strata.gender_concept_id,
          strata.age AS age_at_obs,
          COUNT(person_id) AS n,
          1.0 * COUNT(person_id) / NULLIF(SUM(COUNT(person_id)) OVER(PARTITION BY gender_concept_id),0) AS probability
        FROM v_src_person_strata strata
        GROUP BY gender_concept_id, age) t1;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_age_at_obs_probability.';
    PERFORM insert_log(MESSAGE, 'ins_age_at_obs_probability');

    --COMMIT;
    -- Ensure last accumulated_probability = 1.0
    UPDATE osim_age_at_obs_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY gender_concept_id
            ORDER BY accumulated_probability DESC)
      FROM osim_age_at_obs_probability);

     --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_age_at_obs_probability');
    raise notice 'Processing complete ins_age_at_obs_probability, rows = %', num_rows;

  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_age_at_obs_probability');
    raise notice '% %', SQLERRM, SQLSTATE;
  END;
  $$ LANGUAGE plpgsql;


  CREATE OR REPLACE FUNCTION ins_cond_count_probability()
  /*=========================================================================
  | PROCEDURE INS_COND_COUNT_PROBABILITY
  |
  | Pr(Condition Concepts | Age, Gender)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of Distinct Condition Concepts Probability
  |   Based on Age and Gender
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Condition Concept Count Probability Analysis', 'ins_cond_count_probability');

    TRUNCATE TABLE osim_cond_count_probability;
    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_cond_count_probability
      SELECT
        gender_concept_id,
        age_at_obs,
        NULL AS cond_era_count,
        cond_count AS cond_concept_count,
        n,
        SUM(probability) OVER
         (PARTITION BY gender_concept_id, age_at_obs
          ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
      FROM
       (SELECT DISTINCT
          strata.gender_concept_id,
          strata.age AS age_at_obs,
          strata.condition_concepts AS cond_count,
          COUNT(strata.person_id) AS n,
          1.0 * COUNT(strata.person_id)/ NULLIF(SUM(COUNT(strata.person_id)) OVER(PARTITION BY strata.gender_concept_id, strata.age),0) AS probability
        FROM v_src_person_strata strata
        GROUP BY strata.gender_concept_id, strata.age, strata.condition_concepts) t1
      ORDER BY 1,2,6;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_count_probability.';
    PERFORM insert_log(MESSAGE, 'ins_cond_count_probability');
    raise notice 'Inserted ins_cond_count_probability, rows = %', num_rows;

    --COMMIT;

    UPDATE osim_cond_count_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY gender_concept_id, age_at_obs
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_count_probability);

  --COMMIT;

  PERFORM insert_log('Processing complete', 'ins_cond_count_probability');
  raise notice 'Processing complete ins_cond_count_probability';
  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_cond_count_probability');
    GET STACKED DIAGNOSTICS MESSAGE = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'context: >>%<<', MESSAGE;
    raise notice '% %', SQLERRM, SQLSTATE;

  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION ins_time_obs_probability()
  /*=========================================================================
  | PROCEDURE ins_time_obs_probability
  |
  | Pr(Semi Years Observed | Condition Concept Bucket, Age, Gender)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of Distinct Condition Concepts Probability
  |   Based on Age and Gender
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE     text;
  BEGIN
    PERFORM insert_log('Starting Observed Years Probability Analysis', 'ins_time_obs_probability');

    TRUNCATE TABLE osim_time_obs_probability;
    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_time_obs_probability
      SELECT
        gender_concept_id,
        age_at_obs,
        cond_count_bucket,
        semi_years_obs,
        n,
        SUM(probability) OVER
         (PARTITION BY gender_concept_id, age_at_obs, cond_count_bucket
          ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
      FROM
       (SELECT DISTINCT
          strata.gender_concept_id,
          strata.age AS age_at_obs,
          strata.cond_count_bucket,
          strata.semi_years_obs,
          COUNT(strata.person_id) AS n,
          1.0 * COUNT(strata.person_id)/ NULLIF(SUM(COUNT(strata.person_id)) OVER(PARTITION BY strata.gender_concept_id, strata.age,
              strata.cond_count_bucket), 0) AS probability
        FROM
         (SELECT
            person_id,
            gender_concept_id,
            age,
            OSIM__condition_count_bucket(condition_concepts) AS cond_count_bucket,
            OSIM__time_observed_bucket(obs_duration_days) AS semi_years_obs
          FROM v_src_person_strata) strata
        GROUP BY strata.gender_concept_id, strata.age,
            strata.cond_count_bucket, strata.semi_years_obs) t1
      ORDER BY 1,2,3,6;
    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_years_obs_probability.';
    PERFORM insert_log(MESSAGE, 'ins_time_obs_probability');
    raise notice 'Inserted ins_time_obs_probability, rows = %', num_rows;

    --COMMIT;

    UPDATE osim_time_obs_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY gender_concept_id, age_at_obs, cond_count_bucket
            ORDER BY accumulated_probability DESC)
      FROM osim_time_obs_probability);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_time_obs_probability');
    raise notice 'Processing complete ins_time_obs_probability';

  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_time_obs_probability');
    raise notice '% %', SQLERRM, SQLSTATE;
  END;
  $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_first_cond_probability()
  /*=========================================================================
  | PROCEDURE ins_first_cond_probability
  |
  | Pr(Condition | Age, Gender, Condition Count, Time Remaining, Prior Condition)
  |
  | Analyze source CDM database and store results for:
  |   Initial Distinct Condition Concept Era
  |   Based on INTEGER of Distinct Condition Concepts, Age, Gender, Time
  |     Remaining (unit is integer of full semi years remaining in person's
  |     Observation Period, and Prior Condition Concept
  |
  |   Prior Condition Concept = -1 to retrieve initial Condition Concept
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN

    PERFORM insert_log('Starting First Condition Concept Probability Analysis', 'ins_first_cond_probability');

    TRUNCATE TABLE osim_first_cond_probability;
    --COMMIT;

    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_first_cond_ix1;
      DROP INDEX osim_first_cond_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed', 'ins_first_cond_probability');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_first_cond_probability
    (gender_concept_id, age_range, cond_count_bucket,
      time_remaining, condition1_concept_id, condition2_concept_id,
     delta_days, n, accumulated_probability)
      SELECT
        gender_concept_id,
        age_range,
        cond_count_bucket,
        time_remaining,
        condition1_concept_id,
        condition2_concept_id,
        delta_days,
        n,
        SUM(probability)
          OVER
           (PARTITION BY
              gender_concept_id,
              age_range,
              cond_count_bucket,
              condition1_concept_id,
              time_remaining
            ORDER BY probability DESC
              ROWS UNBOUNDED PRECEDING) accumulated_probability
      FROM
       (SELECT
          gender_concept_id,
          age_range,
          cond_count_bucket,
          time_remaining,
          condition1_concept_id,
          condition2_concept_id,
          delta_days,
          condition_count AS n,
          1.0 * condition_count/ NULLIF(SUM(condition_count) OVER
            (PARTITION BY gender_concept_id, age_range, cond_count_bucket,
              condition1_concept_id, time_remaining), 0)
            AS probability
        FROM
         (SELECT
            gender_concept_id,
            age_range,
            cond_count_bucket,
            count(*) as condition_count,
            time_remaining,
            condition1_concept_id,
            condition2_concept_id,
            delta_days
          FROM (SELECT * from OSIM__get_first_cond_transitions()) t1
          GROUP BY
            gender_concept_id,
            age_range,
            cond_count_bucket,
            time_remaining,
            condition1_concept_id,
            delta_days,
            condition2_concept_id) t2
        ORDER BY
          condition1_concept_id,
          delta_days,
          age_range,
          gender_concept_id,
          cond_count_bucket,
          time_remaining,
          condition_count DESC) t3
      ORDER BY 5,2,1,3,8;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_first_cond_probability.';
    PERFORM insert_log(MESSAGE, 'ins_first_cond_probability');
    raise notice 'Inserted ins_first_cond_probability, rows = %', num_rows;

    --COMMIT;


      CREATE INDEX osim_first_cond_ix1 ON osim_first_cond_probability (
        condition1_concept_id, age_range, gender_concept_id,
        cond_count_bucket, time_remaining)
      WITH (FILLFACTOR = 90);


      CREATE INDEX osim_first_cond_ix2
        ON osim_first_cond_probability (accumulated_probability)
      WITH (FILLFACTOR = 90);

    --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_first_cond_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition1_concept_id, age_range, gender_concept_id,
              cond_count_bucket, time_remaining
            ORDER BY accumulated_probability DESC)
      FROM osim_first_cond_probability);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_first_cond_probability');
    raise notice 'Processing complete ins_first_cond_probability';

  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_first_cond_probability');
    raise notice '% %', SQLERRM, SQLSTATE;

  END;
  $$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION ins_cond_days_before_prob()
  /*=========================================================================
  | PROCEDURE ins_cond_days_before_probins_cond_days_before_prob
  |
  | Pr(Days Until | Condition, Age Bucket, Time Remaining)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of days from Prior Condition Era (for same Condition Concept)
  |   Based on Condition, Age, and Full Semi-years Remaining in
  |     Person's Observation Period
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Condition Concept Reoccurrence Probability Analysis',
      'ins_cond_reoccur_probability');

    TRUNCATE TABLE osim_cond_reoccur_probability;
    --COMMIT;

    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_cond_reoccur_ix1;
      DROP INDEX osim_cond_reoccur_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_cond_reoccur_probability');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_cond_reoccur_probability
    (condition_concept_id, age_range, time_remaining,
     delta_days, n, accumulated_probability)
      SELECT
        condition_concept_id,
        age_range,
        time_remaining,
        delta_days,
        n,
        SUM(probability)
          OVER
           (PARTITION BY
              --gender_concept_id,
              age_range,
              time_remaining,
              condition_concept_id
            ORDER BY probability DESC
              ROWS UNBOUNDED PRECEDING) accumulated_probability
      FROM
       (SELECT
          --gender_concept_id,
          age_range,
          time_remaining,
          condition_concept_id,
          delta_days,
          eras AS n,
          1.0 * eras/ NULLIF(SUM(eras)
                                OVER(PARTITION BY age_range, time_remaining, condition_concept_id), 0)
            AS probability
        FROM
         (SELECT
            --gender_concept_id,
            age_range,
            time_remaining,
            condition_concept_id,
            delta_days,
            count(condition_era_id) AS eras
          FROM
           (SELECT
              condition_era_id,
              condition_concept_id,
              osim__age_bucket(age + (prior_start - this_start) / 365.25)
                AS age_range,
              osim__time_observed_bucket(observation_period_end_date - prior_start)
                AS time_remaining,
              osim__round_days(this_start - prior_start) AS delta_days
            FROM
             (SELECT
                person.age,
                cond.condition_era_id,
                cond.condition_concept_id,
                coalesce(LAG(cond.condition_era_start_date,1)
                  OVER ( PARTITION BY person.person_id, cond.condition_concept_id
                         ORDER BY cond.condition_era_start_date),
                          person.observation_period_start_date) AS prior_start,
                cond.condition_era_start_date AS this_start,
                person.observation_period_end_date
              FROM v_src_person_strata person
              INNER JOIN v_src_condition_era1 cond
                ON person.person_id = cond.person_id) t1
           ) t2
          GROUP BY age_range, time_remaining, condition_concept_id, delta_days) t3
       ) t4
      ORDER BY 1,2,3,6;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_reoccur_probability.';
    PERFORM insert_log(MESSAGE, 'ins_cond_reoccur_probability');
    raise notice 'Inserted ins_cond_reoccur_probability, rows = %', num_rows;

    --COMMIT;


    CREATE INDEX osim_cond_reoccur_ix1 ON osim_cond_reoccur_probability (
      condition_concept_id,
      age_range,
      time_remaining)
    WITH (FILLFACTOR = 90);


    CREATE INDEX osim_cond_reoccur_ix2 ON osim_cond_reoccur_probability (
      accumulated_probability)
    WITH (FILLFACTOR = 90);

    --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_cond_reoccur_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition_concept_id, age_range, time_remaining
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_reoccur_probability);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_cond_reoccur_probability');
    raise notice 'Processing complete ins_cond_reoccur_probability';

    EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_cond_reoccur_probability');

  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_procedure_days_before_prob()
  /*=========================================================================
  | PROCEDURE ins_procedure_days_before_prob
  |
  | Pr(Days Until | procedure, Age Bucket, Time Remaining)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of days from Prior procedure occurrence (for same procedure Concept)
  |   Based on procedure, Age, and Full Semi-years Remaining in
  |     Person's Observation Period
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Procedure Concept Reoccurrence Probability Analysis',
      'ins_procedure_reoccur_probability');

    TRUNCATE TABLE osim_procedure_reoccur_probability;
    --COMMIT;

    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_procedure_reoccur_ix1;
      DROP INDEX osim_procedure_reoccur_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_procedure_reoccur_probability');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_procedure_reoccur_probability
    (procedure_concept_id, age_range, time_remaining,
     delta_days, n, accumulated_probability)
      SELECT
        procedure_concept_id,
        age_range,
        time_remaining,
        delta_days,
        n,
        SUM(probability)
          OVER
           (PARTITION BY
              --gender_concept_id,
              age_range,
              time_remaining,
              procedure_concept_id
            ORDER BY probability DESC
              ROWS UNBOUNDED PRECEDING) accumulated_probability
      FROM
       (SELECT
          --gender_concept_id,
          age_range,
          time_remaining,
          procedure_concept_id,
          delta_days,
          occurrences AS n,
          1.0 * occurrences/ NULLIF(SUM(occurrences)
                                OVER(PARTITION BY age_range, time_remaining, procedure_concept_id), 0)
            AS probability
        FROM
         (SELECT
            --gender_concept_id,
            age_range,
            time_remaining,
            procedure_concept_id,
            delta_days,
            count(procedure_occurrence_id) AS occurrences
          FROM
           (SELECT
              procedure_occurrence_id,
              procedure_concept_id,
              osim__age_bucket(age + (prior_start - this_start) / 365.25)
                AS age_range,
              osim__time_observed_bucket(observation_period_end_date - prior_start)
                AS time_remaining,
              osim__round_days(this_start - prior_start) AS delta_days
            FROM
             (SELECT
                person.age,
                procedure.procedure_occurrence_id,
                procedure.procedure_concept_id,
                coalesce(LAG(procedure.procedure_date,1)
                  OVER ( PARTITION BY person.person_id, procedure.procedure_concept_id
                         ORDER BY procedure.procedure_date),
                          person.observation_period_start_date) AS prior_start,
                procedure.procedure_date AS this_start,
                person.observation_period_end_date
              FROM v_src_person_strata person
              INNER JOIN v_src_procedure_occurrence1 procedure
                ON person.person_id = procedure.person_id) t1
           ) t2
          GROUP BY age_range, time_remaining, procedure_concept_id, delta_days) t3
       ) t4
      ORDER BY 1,2,3,6;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_procedure_reoccur_probability.';
    PERFORM insert_log(MESSAGE, 'ins_procedure_reoccur_probability');
    raise notice 'Inserted ins_procedure_reoccur_probability, rows = %', num_rows;

    --COMMIT;


    CREATE INDEX osim_procedure_reoccur_ix1 ON osim_procedure_reoccur_probability (
      procedure_concept_id,
      age_range,
      time_remaining)
    WITH (FILLFACTOR = 90);


    CREATE INDEX osim_procedure_reoccur_ix2 ON osim_procedure_reoccur_probability (
      accumulated_probability)
    WITH (FILLFACTOR = 90);

    --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_procedure_reoccur_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY procedure_concept_id, age_range, time_remaining
            ORDER BY accumulated_probability DESC)
      FROM osim_procedure_reoccur_probability);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_procedure_reoccur_probability');
    raise notice 'Processing complete ins_procedure_reoccur_probability';

    EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_procedure_reoccur_probability');

  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_drug_count_prob()
  /*=========================================================================
  | PROCEDURE ins_drug_count_prob
  |
  | Pr(Drug Concept Count | Gender, Age Bucket, Condition Concept Count Bucket)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of Distinct Drug Concepts based on Person's INTEGER of
  |   Gender
  |   Age
  |   Distinct Condition Count Bucket
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Drug Concept Count Probability Analysis',
      'ins_drug_count_prob');

    TRUNCATE TABLE osim_drug_count_prob;

    --COMMIT;
    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_drug_count_prob_ix1;
      DROP INDEX osim_drug_count_prob_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_drug_count_prob');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_drug_count_prob
    ( gender_concept_id, age_bucket, condition_count_bucket,
      drug_count, n, accumulated_probability )
    SELECT
      gender_concept_id,
      age_bucket,
      condition_count_bucket,
      drug_concepts AS drug_count,
      n,
      SUM(probability)
        OVER
         (PARTITION BY gender_concept_id, age_bucket, condition_count_bucket
          ORDER BY probability DESC
            ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        gender_concept_id,
        age_bucket,
        condition_count_bucket,
        drug_concepts,
        persons AS n,
        1.0 * persons/ NULLIF(SUM(persons)
                                OVER(PARTITION BY gender_concept_id, age_bucket, condition_count_bucket), 0)
            AS probability
      FROM
       (SELECT
          gender_concept_id,
          age_bucket,
          condition_count_bucket,
          drug_concepts,
          COUNT(person_id) AS persons
        FROM
         (SELECT
            person_id,
            gender_concept_id,
            osim__age_bucket(age) AS age_bucket,
            osim__condition_count_bucket(condition_concepts) AS condition_count_bucket,
            drug_concepts
          FROM v_src_person_strata person) t1
        GROUP BY gender_concept_id, age_bucket, condition_count_bucket, drug_concepts) t2
     ) t3;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_drug_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_drug_count_prob');
    raise notice 'Inserted ins_drug_count_prob, rows = %', num_rows;

    --COMMIT;


    CREATE INDEX osim_drug_count_prob_ix1 ON osim_drug_count_prob (
      gender_concept_id, age_bucket, condition_count_bucket)
    WITH (FILLFACTOR = 90);


    CREATE INDEX osim_drug_count_prob_ix2
      ON osim_drug_count_prob (accumulated_probability)
    WITH (FILLFACTOR = 90);


    --COMMIT;
    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_drug_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY gender_concept_id, age_bucket, condition_count_bucket
            ORDER BY accumulated_probability DESC)
      FROM osim_drug_count_prob);

     --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_drug_count_prob');
    raise notice 'Processing complete ins_drug_count_prob';

    EXCEPTION
      WHEN OTHERS THEN
      PERFORM insert_log('Exception', 'ins_drug_count_prob');
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_procedure_count_prob()
  /*=========================================================================
  | PROCEDURE ins_procedure_count_prob
  |
  | Pr(Procedure Concept Count | Gender, Age Bucket, Condition Concept Count Bucket)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of Distinct Procedure Concepts based on Person's INTEGER of
  |   Gender
  |   Age
  |   Distinct Condition Count Bucket
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Procedure Concept Count Probability Analysis',
      'ins_procedure_count_prob');

    TRUNCATE TABLE osim_procedure_count_prob;

    --COMMIT;
    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_procedure_count_prob_ix1;
      DROP INDEX osim_procedure_count_prob_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_procedure_count_prob');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_procedure_count_prob
    ( gender_concept_id, age_bucket, condition_count_bucket,
      procedure_count, n, accumulated_probability )
    SELECT
      gender_concept_id,
      age_bucket,
      condition_count_bucket,
      procedure_concepts AS procedure_count,
      n,
      SUM(probability)
        OVER
         (PARTITION BY gender_concept_id, age_bucket, condition_count_bucket
          ORDER BY probability DESC
            ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        gender_concept_id,
        age_bucket,
        condition_count_bucket,
        procedure_concepts,
        persons AS n,
        1.0 * persons/ NULLIF(SUM(persons)
                                OVER(PARTITION BY gender_concept_id, age_bucket, condition_count_bucket), 0)
            AS probability
      FROM
       (SELECT
          gender_concept_id,
          age_bucket,
          condition_count_bucket,
          procedure_concepts,
          COUNT(person_id) AS persons
        FROM
         (SELECT
            person_id,
            gender_concept_id,
            osim__age_bucket(age) AS age_bucket,
            osim__condition_count_bucket(condition_concepts) AS condition_count_bucket,
            procedure_concepts
          FROM v_src_person_strata person) t1
        GROUP BY gender_concept_id, age_bucket, condition_count_bucket, procedure_concepts) t2
     ) t3;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_drug_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_drug_count_prob');
    raise notice 'Inserted ins_drug_count_prob, rows = %', num_rows;

    --COMMIT;


    CREATE INDEX osim_procedure_count_prob_ix1 ON osim_procedure_count_prob (
      gender_concept_id, age_bucket, condition_count_bucket)
    WITH (FILLFACTOR = 90);


    CREATE INDEX osim_procedure_count_prob_ix2
      ON osim_procedure_count_prob (accumulated_probability)
    WITH (FILLFACTOR = 90);


    --COMMIT;
    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_procedure_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY gender_concept_id, age_bucket, condition_count_bucket
            ORDER BY accumulated_probability DESC)
      FROM osim_procedure_count_prob);

     --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_procedure_count_prob');
    raise notice 'Processing complete ins_procedure_count_prob';

    EXCEPTION
      WHEN OTHERS THEN
      PERFORM insert_log('Exception', 'ins_procedure_count_prob');
      GET STACKED DIAGNOSTICS MESSAGE = PG_EXCEPTION_CONTEXT;
      RAISE NOTICE 'context: >>%<<', MESSAGE;
      raise notice '% %', SQLERRM, SQLSTATE;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_cond_drug_count_prob()
  /*=========================================================================
  | PROCEDURE ins_cond_drug_count_prob
  |
  | Pr(Drug Draw Count | Condition Concept, Interval Bucket,
  |    Drug Concept Count Bucket, Condition Concept Count Bucket)
  |
  | This is the INTEGER of times to draw for a first occurrence drug era for
  | a condition era
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of Drug Eras from a condition to the next day with conditions
  |   based on Condition Concept, the Bucketed Duration until the next day
  |   with a Conditon Era, Person's Distinct Drug Count Bucket, and Person's
  |   Distinct Condition Count Bucket
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Condition Interval Drug Era Count Probability Analysis',
      'ins_cond_drug_count_prob');

    TRUNCATE TABLE osim_cond_drug_count_prob;
    --COMMIT;

    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_cond_drug_count_prob_ix1;
      DROP INDEX osim_cond_drug_count_prob_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_cond_drug_count_prob');
    END;

     --COMMIT;


    INSERT /*+ append nologging */ INTO osim_cond_drug_count_prob
    (  condition_concept_id, interval_bucket, age_bucket, drug_count_bucket,
        condition_count_bucket, drug_count, n, accumulated_probability )
    WITH gaps AS
     (SELECT
       cond_gaps.person_id,
       cond_gaps.age_bucket,
       cond_gaps.cond_start_date,
       cond_gaps.next_cond_start_date,
       cond_gaps.drug_concepts,
       cond_gaps.condition_concepts,
       cond_gaps.day_cond_count,
       coalesce(COUNT(DISTINCT drug.drug_concept_id),0) AS drug_count
      FROM
       (SELECT
          cond_dates.person_id,
          cond_dates.condition_era_start_date AS cond_start_date,
          person.drug_concepts,
          person.condition_concepts,
          osim__age_bucket(
            person.age + (cond_dates.condition_era_start_date
              - person.observation_period_start_date) / 365.25)
                      AS age_bucket,
          LEAD (cond_dates.condition_era_start_date,1,
              person.observation_period_end_date)
            OVER (PARTITION BY cond_dates.person_id
                  ORDER BY cond_dates.condition_era_start_date)
                    AS next_cond_start_date,
          cond_dates.day_cond_count
        FROM v_src_person_strata person
        INNER JOIN
         (SELECT
            person_id,
            condition_era_start_date,
            COUNT(condition_concept_id) AS day_cond_count
          FROM
           (SELECT
              cond.person_id,
              cond.condition_concept_id,
              cond.condition_era_start_date
            FROM v_src_condition_era1 cond
            UNION
            SELECT
              person_id,
              -1 AS condition_concept_id,
              observation_period_start_date AS condition_era_start_date
            FROM v_src_person_strata) t1
          GROUP BY person_id, condition_era_start_date) cond_dates
          ON person.person_id = cond_dates.person_id
        WHERE person.observation_period_end_date
          >= cond_dates.condition_era_start_date) cond_gaps
      LEFT JOIN v_src_first_drugs drug ON cond_gaps.person_id = drug.person_id
          AND cond_gaps.cond_start_date <= drug.drug_era_start_date
          AND cond_gaps.next_cond_start_date > drug.drug_era_start_date
      GROUP BY cond_gaps.person_id, cond_gaps.age_bucket, cond_gaps.cond_start_date,
        cond_gaps.next_cond_start_date, cond_gaps.drug_concepts,
        cond_gaps.condition_concepts,cond_gaps.day_cond_count)
    SELECT
      condition_concept_id,
      interval_bucket,
      age_bucket,
      drug_count_bucket,
      condition_count_bucket,
      gap_drug_count,
      n,
      SUM(probability)
        OVER
         (PARTITION BY condition_concept_id, interval_bucket, age_bucket,
                   drug_count_bucket, condition_count_bucket
          ORDER BY probability DESC
            ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        condition_concept_id,
        interval_bucket,
        age_bucket,
        drug_count_bucket,
        condition_count_bucket,
        gap_drug_count,
        sum_prob AS n,
        1.0 * sum_prob/ NULLIF(SUM(sum_prob)
                                OVER(PARTITION BY condition_concept_id, interval_bucket, age_bucket,
                   drug_count_bucket, condition_count_bucket), 0) AS probability
      FROM
       (SELECT
          condition_concept_id,
          interval_bucket,
          age_bucket,
          drug_count_bucket,
          condition_count_bucket,
          gap_drug_count,
          SUM(prob) AS sum_prob
        FROM
         (SELECT DISTINCT
            gaps.person_id,
            gaps.age_bucket,
            cond.condition_concept_id,
            gaps.cond_start_date,
            gaps.next_cond_start_date,
            osim__drug_count_bucket(gaps.drug_concepts) AS drug_count_bucket,
            osim__condition_count_bucket(gaps.condition_concepts)
              AS condition_count_bucket,
            osim__duration_days_bucket(gaps.next_cond_start_date
                - gaps.cond_start_date) AS interval_bucket,
            1 AS prob,
            gaps.drug_count as gap_drug_count
          FROM gaps
          INNER JOIN v_src_condition_era1 cond
            ON gaps.person_id = cond.person_id
            AND gaps.cond_start_date = cond.condition_era_start_date
          UNION ALL
          SELECT DISTINCT
            person_id,
            age_bucket,
            -1 AS condition_concept_id,
            FIRST_VALUE(cond_start_date)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
                AS cond_start_date,
            FIRST_VALUE(next_cond_start_date)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
                AS next_cond_start_date,
            osim__drug_count_bucket(drug_concepts)
                AS drug_count_bucket,
            osim__condition_count_bucket(condition_concepts)
                AS condition_count_bucket,
            osim__duration_days_bucket(FIRST_VALUE(next_cond_start_date)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
            - FIRST_VALUE(cond_start_date)
                OVER (PARTITION BY person_id ORDER BY cond_start_date))
                  AS interval_bucket,
            1 AS prob,
            FIRST_VALUE(drug_count)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
                AS gap_drug_count
          FROM gaps
          ORDER BY 1,3,2) t1
        GROUP BY condition_concept_id, interval_bucket, age_bucket, drug_count_bucket,
          condition_count_bucket, gap_drug_count
      ORDER BY 1,2,3,4,5,7 DESC) t2
     ) t3
    ORDER BY 1,2,3,4,5,8;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_drug_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_cond_drug_count_prob');
    raise notice 'Inserted ins_cond_drug_count_prob, rows = %', num_rows;

     --COMMIT;


    CREATE INDEX osim_cond_drug_count_prob_ix1 ON osim_cond_drug_count_prob (
      condition_concept_id, interval_bucket, age_bucket, drug_count_bucket,
      condition_count_bucket)
    WITH (FILLFACTOR = 90);


    CREATE INDEX osim_cond_drug_count_prob_ix2
      ON osim_cond_drug_count_prob (accumulated_probability)
    WITH (FILLFACTOR = 90);

    --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_cond_drug_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition_concept_id, interval_bucket, age_bucket,
              drug_count_bucket, condition_count_bucket
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_drug_count_prob);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_cond_drug_count_prob');
    raise notice 'Processing complete ins_cond_drug_count_prob';
    EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_cond_drug_count_prob');
  raise notice 'Processing complete ins_cond_count_probability, rows = %', num_rows;

  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_cond_procedure_count_prob()
  /*=========================================================================
  | PROCEDURE ins_cond_procedure_count_prob
  |
  | Pr(Procedure Draw Count | Condition Concept, Interval Bucket,
  |    procedure Concept Count Bucket, Condition Concept Count Bucket)
  |
  | This is the INTEGER of times to draw for a first occurrence procedure era for
  | a condition era
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of procedure Occurrences from a condition to the next day with conditions
  |   based on Condition Concept, the Bucketed Duration until the next day
  |   with a Conditon Era, Person's Distinct procedure Count Bucket, and Person's
  |   Distinct Condition Count Bucket
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Condition Interval Procedure Occurrences Count Probability Analysis',
      'ins_cond_procedure_count_prob');

    TRUNCATE TABLE osim_cond_procedure_count_prob;
    --COMMIT;

    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_cond_procedure_count_prob_ix1;
      DROP INDEX osim_cond_procedure_count_prob_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_cond_procedure_count_prob');
    END;

     --COMMIT;


    INSERT /*+ append nologging */ INTO osim_cond_procedure_count_prob
    (  condition_concept_id, interval_bucket, age_bucket, procedure_count_bucket,
        condition_count_bucket, procedure_count, n, accumulated_probability )
    WITH gaps AS
     (SELECT
       cond_gaps.person_id,
       cond_gaps.age_bucket,
       cond_gaps.cond_start_date,
       cond_gaps.next_cond_start_date,
       cond_gaps.procedure_concepts,
       cond_gaps.condition_concepts,
       cond_gaps.day_cond_count,
       coalesce(COUNT(DISTINCT procedure.procedure_concept_id),0) AS procedure_count
      FROM
       (SELECT
          cond_dates.person_id,
          cond_dates.condition_era_start_date AS cond_start_date,
          person.procedure_concepts,
          person.condition_concepts,
          osim__age_bucket(
            person.age + (cond_dates.condition_era_start_date
              - person.observation_period_start_date) / 365.25)
                      AS age_bucket,
          LEAD (cond_dates.condition_era_start_date,1,
              person.observation_period_end_date)
            OVER (PARTITION BY cond_dates.person_id
                  ORDER BY cond_dates.condition_era_start_date)
                    AS next_cond_start_date,
          cond_dates.day_cond_count
        FROM v_src_person_strata person
        INNER JOIN
         (SELECT
            person_id,
            condition_era_start_date,
            COUNT(condition_concept_id) AS day_cond_count
          FROM
           (SELECT DISTINCT
              cond.person_id,
              cond.condition_concept_id,
              cond.condition_era_start_date
            FROM v_src_condition_era1 cond
            UNION
            SELECT
              person_id,
              -1 AS condition_concept_id,
              observation_period_start_date AS condition_era_start_date
            FROM v_src_person_strata) t1
          GROUP BY person_id, condition_era_start_date) cond_dates
          ON person.person_id = cond_dates.person_id
        WHERE person.observation_period_end_date
          >= cond_dates.condition_era_start_date) cond_gaps
      LEFT JOIN v_src_first_procedures procedure ON cond_gaps.person_id = procedure.person_id
          AND cond_gaps.cond_start_date <= procedure.procedure_date
          AND cond_gaps.next_cond_start_date > procedure.procedure_date
      GROUP BY cond_gaps.person_id, cond_gaps.age_bucket, cond_gaps.cond_start_date,
        cond_gaps.next_cond_start_date, cond_gaps.procedure_concepts,
        cond_gaps.condition_concepts,cond_gaps.day_cond_count)
    SELECT
      condition_concept_id,
      interval_bucket,
      age_bucket,
      procedure_count_bucket,
      condition_count_bucket,
      gap_procedure_count,
      n,
      SUM(probability)
        OVER
         (PARTITION BY condition_concept_id, interval_bucket, age_bucket,
                   procedure_count_bucket, condition_count_bucket
          ORDER BY probability DESC
            ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        condition_concept_id,
        interval_bucket,
        age_bucket,
        procedure_count_bucket,
        condition_count_bucket,
        gap_procedure_count,
        sum_prob AS n,
        1.0 * sum_prob/ NULLIF(SUM(sum_prob)
                                OVER(PARTITION BY condition_concept_id, interval_bucket, age_bucket,
                   procedure_count_bucket, condition_count_bucket), 0) AS probability
      FROM
       (SELECT
          condition_concept_id,
          interval_bucket,
          age_bucket,
          procedure_count_bucket,
          condition_count_bucket,
          gap_procedure_count,
          SUM(prob) AS sum_prob
        FROM
         (SELECT DISTINCT
            gaps.person_id,
            gaps.age_bucket,
            cond.condition_concept_id,
            gaps.cond_start_date,
            gaps.next_cond_start_date,
            osim__procedure_count_bucket(gaps.procedure_concepts) AS procedure_count_bucket,
            osim__condition_count_bucket(gaps.condition_concepts)
              AS condition_count_bucket,
            osim__duration_days_bucket(gaps.next_cond_start_date
                - gaps.cond_start_date) AS interval_bucket,
            1 AS prob,
            gaps.procedure_count as gap_procedure_count
          FROM gaps
          INNER JOIN v_src_condition_era1 cond
            ON gaps.person_id = cond.person_id
            AND gaps.cond_start_date = cond.condition_era_start_date
          UNION ALL
          SELECT DISTINCT
            person_id,
            age_bucket,
            -1 AS condition_concept_id,
            FIRST_VALUE(cond_start_date)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
                AS cond_start_date,
            FIRST_VALUE(next_cond_start_date)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
                AS next_cond_start_date,
            osim__procedure_count_bucket(procedure_concepts)
                AS procedure_count_bucket,
            osim__condition_count_bucket(condition_concepts)
                AS condition_count_bucket,
            osim__duration_days_bucket(FIRST_VALUE(next_cond_start_date)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
            - FIRST_VALUE(cond_start_date)
                OVER (PARTITION BY person_id ORDER BY cond_start_date))
                  AS interval_bucket,
            1 AS prob,
            FIRST_VALUE(procedure_count)
              OVER (PARTITION BY person_id ORDER BY cond_start_date)
                AS gap_procedure_count
          FROM gaps
          ORDER BY 1,3,2) t1
        GROUP BY condition_concept_id, interval_bucket, age_bucket, procedure_count_bucket,
          condition_count_bucket, gap_procedure_count
      ORDER BY 1,2,3,4,5,7 DESC) t2
     ) t3
    ORDER BY 1,2,3,4,5,8;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_procedure_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_cond_procedure_count_prob');
    raise notice 'Inserted ins_cond_procedure_count_prob, rows = %', num_rows;

     --COMMIT;


    CREATE INDEX osim_cond_procedure_count_prob_ix1 ON osim_cond_procedure_count_prob (
      condition_concept_id, interval_bucket, age_bucket, procedure_count_bucket,
      condition_count_bucket)
    WITH (FILLFACTOR = 90);


    CREATE INDEX osim_cond_procedure_count_prob_ix2
      ON osim_cond_procedure_count_prob (accumulated_probability)
    WITH (FILLFACTOR = 90);

    --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_cond_procedure_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition_concept_id, interval_bucket, age_bucket,
              procedure_count_bucket, condition_count_bucket
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_procedure_count_prob);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_cond_procedure_count_prob');
    raise notice 'Processing complete ins_cond_procedure_count_prob';
    EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_cond_procedure_count_prob');
  raise notice 'Processing complete ins_cond_count_probability, rows = %', num_rows;

  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_cond_first_drug_prob()
  /*=========================================================================
  | PROCEDURE ins_cond_first_drug_prob
  |
  | Pr(Drug Concept, Days until | Condition Concept, Interval Bucket,
  |    Drug Concept Count Bucket, Condition Concept Count Bucket,
  |    INTEGER of Conditons with the same Start Date, Gender)
  |
  | Analyze source CDM database and store results for:
  |   Probability of Drug Concept and Days until for every Condition Era
  |     based on Conditon Concept, the Bucketed Duration until the next day
  |     with a Conditon Era, Person's Distinct Drug Count Bucket, and Person's
  |     Distinct Condition Count Bucket, the INTEGER of Condition Eras the
  |     the Person has with the Same Start Date, Person's Gender
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Condition Interval Drug Era Analysis',
      'ins_cond_first_drug_prob');

    TRUNCATE TABLE osim_cond_first_drug_prob;

    --COMMIT;
    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_cond_drug_prob_ix1;
      DROP INDEX osim_cond_drug_prob_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_cond_first_drug_prob');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_cond_first_drug_prob
    (  condition_concept_id, interval_bucket, gender_concept_id, age_bucket,
        condition_count_bucket, drug_count_bucket, day_cond_count, drug_concept_id,
        delta_days, n, accumulated_probability )
    WITH gaps AS
     (SELECT
       cond_gaps.person_id,
       cond_gaps.gender_concept_id,
       cond_gaps.age_bucket,
       cond_gaps.condition_count_bucket,
       cond_gaps.drug_count_bucket,
       cond_gaps.cond_start_date,
       cond_gaps.next_cond_start_date,
       cond_gaps.day_cond_count,
       coalesce(COUNT(DISTINCT drug.drug_concept_id),0) AS drug_era_count
      FROM
       (SELECT
          cond_dates.person_id,
          person.gender_concept_id,
          osim__age_bucket(
            person.age + (cond_dates.condition_era_start_date
              - person.observation_period_start_date)
              / 365.25) AS age_bucket,
          osim__condition_count_bucket(person.condition_concepts)
            AS condition_count_bucket,
          osim__drug_count_bucket(person.drug_concepts) AS drug_count_bucket,
          cond_dates.condition_era_start_date AS cond_start_date,
          LEAD (cond_dates.condition_era_start_date,1,person.observation_period_end_date)
            OVER (PARTITION BY cond_dates.person_id
                  ORDER BY cond_dates.condition_era_start_date)
                    AS next_cond_start_date,
          cond_dates.day_cond_count
        FROM v_src_person_strata person
        INNER JOIN
         (SELECT
            person_id,
            condition_era_start_date,
            COUNT(condition_concept_id) AS day_cond_count
          FROM
           (SELECT
              cond.person_id,
              cond.condition_concept_id,
              cond.condition_era_start_date
            FROM v_src_condition_era1 cond
            UNION
            SELECT
              person_id,
              -1 AS condition_concept_id,
              observation_period_start_date AS condition_era_start_date
            FROM v_src_person_strata) t1
          GROUP BY person_id, condition_era_start_date) cond_dates
          ON person.person_id = cond_dates.person_id
        WHERE person.observation_period_end_date
                >= cond_dates.condition_era_start_date) cond_gaps
      LEFT JOIN v_src_first_drugs drug ON cond_gaps.person_id = drug.person_id
          AND cond_gaps.cond_start_date <= drug.drug_era_start_date
          AND cond_gaps.next_cond_start_date > drug.drug_era_start_date
      GROUP BY cond_gaps.person_id, cond_gaps.gender_concept_id,
       cond_gaps.age_bucket, cond_gaps.condition_count_bucket,
       cond_gaps.drug_count_bucket,cond_gaps.cond_start_date,
        cond_gaps.next_cond_start_date, cond_gaps.day_cond_count),
      cond_drug AS
       (SELECT DISTINCT
          gaps.person_id,
          gaps.gender_concept_id,
          gaps.age_bucket,
          gaps.condition_count_bucket,
          gaps.drug_count_bucket,
          gaps.cond_start_date,
          gaps.next_cond_start_date,
          gaps.day_cond_count,
          coalesce(cond.condition_concept_id,-1) AS condition_concept_id,
          coalesce(drug.drug_era_start_date,gaps.cond_start_date) AS drug_era_start_date,
          coalesce(drug.drug_concept_id,-1) AS drug_concept_id
        FROM gaps
        LEFT JOIN v_src_condition_era1 cond
          ON gaps.person_id = cond.person_id
          AND gaps.cond_start_date = cond.condition_era_start_date
        LEFT JOIN v_src_first_drugs drug
          ON gaps.person_id = drug.person_id
            AND gaps.cond_start_date <= drug.drug_era_start_date
            AND gaps.next_cond_start_date > drug.drug_era_start_date)
    SELECT
      condition_concept_id,
      interval_bucket,
      gender_concept_id,
      age_bucket,
      condition_count_bucket,
      drug_count_bucket,
      2 as day_cond_count,
      drug_concept_id,
      delta_days,
      n,
      SUM(probability)
        OVER
         (PARTITION BY condition_concept_id, interval_bucket, gender_concept_id,
            age_bucket, condition_count_bucket, drug_count_bucket
          ORDER BY probability DESC
            ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        condition_concept_id,
        interval_bucket,
        gender_concept_id,
        age_bucket,
        condition_count_bucket,
        drug_count_bucket,
        drug_concept_id,
        delta_days,
        sum_prob AS n,
        1.0 * sum_prob/ NULLIF(SUM(sum_prob)
                                OVER(PARTITION BY condition_concept_id, interval_bucket,
                    gender_concept_id, age_bucket, condition_count_bucket,
                    drug_count_bucket), 0) AS probability
      FROM
       (SELECT
          condition_concept_id,
          interval_bucket,
          gender_concept_id,
          age_bucket,
          condition_count_bucket,
          drug_count_bucket,
          delta_days,
          drug_concept_id,
          SUM(prob) AS sum_prob
        FROM
         (SELECT
            condition_concept_id,
            osim__duration_days_bucket(next_cond_start_date - cond_start_date)
              AS interval_bucket,
            gender_concept_id,
            age_bucket,
            condition_count_bucket,
            drug_count_bucket,
            osim__round_days(delta_days) AS delta_days,
            drug_concept_id,
            1.0 / (day_cond_count) AS prob
          FROM
           (SELECT DISTINCT
              person_id,
              gender_concept_id,
              age_bucket,
              condition_count_bucket,
              drug_count_bucket,
              cond_start_date,
              next_cond_start_date,
              day_cond_count,
              condition_concept_id,
              drug_era_start_date - cond_start_date AS delta_days,
              drug_concept_id
            FROM cond_drug
            UNION ALL
            SELECT DISTINCT
              gaps.person_id,
              gaps.gender_concept_id,
              gaps.age_bucket,
              gaps.condition_count_bucket,
              gaps.drug_count_bucket,
              gaps.cond_start_date,
              gaps.next_cond_start_date,
              gaps.day_cond_count,
              cond.condition_concept_id,
              0 AS delta_days,
              -1 AS drug_concept_id
            FROM gaps
            INNER JOIN v_src_condition_era1 cond
              ON gaps.person_id = cond.person_id
                AND gaps.cond_start_date = cond.condition_era_start_date
            WHERE gaps.day_cond_count > 1) t1
          ORDER BY 1,2) t2
        GROUP BY condition_concept_id, interval_bucket, gender_concept_id,
              age_bucket, condition_count_bucket, drug_count_bucket,
              delta_days, drug_concept_id
        ORDER BY 1,2,5 DESC) t3
     ) t4
    ORDER BY 1,2,6;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_drug_prob for '
        || 'co-occurent condtion transitions.';
    PERFORM insert_log(MESSAGE, 'ins_cond_drug_prob');
  raise notice 'Inserted ins_cond_drug_prob, rows = %', num_rows;

    --COMMIT;


    CREATE INDEX osim_cond_drug_prob_ix1 ON osim_cond_first_drug_prob (
      condition_concept_id, interval_bucket, age_bucket, condition_count_bucket,
      drug_count_bucket, day_cond_count, gender_concept_id)
      WITH (FILLFACTOR = 90);


    CREATE INDEX osim_cond_drug_prob_ix2
      ON osim_cond_first_drug_prob (accumulated_probability)
      WITH (FILLFACTOR = 90);

    --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_cond_first_drug_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition_concept_id, interval_bucket, day_cond_count
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_first_drug_prob);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_cond_first_drug_prob');
    raise notice 'Processing complete ins_cond_drug_prob';
    EXCEPTION
      WHEN OTHERS THEN
      PERFORM insert_log('Exception', 'ins_cond_first_drug_prob');
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_cond_first_procedure_prob()
  /*=========================================================================
  | PROCEDURE ins_cond_first_procedure_prob
  |
  | Pr(Procedure Concept, Days until | Condition Concept, Interval Bucket,
  |    Procedure Concept Count Bucket, Condition Concept Count Bucket,
  |    INTEGER of Conditons with the same Start Date, Gender)
  |
  | Analyze source CDM database and store results for:
  |   Probability of Procedure Concept and Days until for every Condition Era
  |     based on Conditon Concept, the Bucketed Duration until the next day
  |     with a Conditon Era, Person's Distinct Procedure Count Bucket, and Person's
  |     Distinct Condition Count Bucket, the INTEGER of Condition Eras the
  |     the Person has with the Same Start Date, Person's Gender
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Condition Interval Procedure Era Analysis',
      'ins_cond_first_procedure_prob');

    TRUNCATE TABLE osim_cond_first_procedure_prob;

    --COMMIT;
    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_cond_procedure_prob_ix1;
      DROP INDEX osim_cond_procedure_prob_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_cond_first_procedure_prob');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_cond_first_procedure_prob
    (  condition_concept_id, interval_bucket, gender_concept_id, age_bucket,
        condition_count_bucket, procedure_count_bucket, day_cond_count, procedure_concept_id,
        delta_days, n, accumulated_probability )
    WITH gaps AS
     (SELECT
       cond_gaps.person_id,
       cond_gaps.gender_concept_id,
       cond_gaps.age_bucket,
       cond_gaps.condition_count_bucket,
       cond_gaps.procedure_count_bucket,
       cond_gaps.cond_start_date,
       cond_gaps.next_cond_start_date,
       cond_gaps.day_cond_count,
       coalesce(COUNT(DISTINCT procedure.procedure_concept_id),0) AS procedure_occurrence_count
      FROM
       (SELECT
          cond_dates.person_id,
          person.gender_concept_id,
          osim__age_bucket(
            person.age + (cond_dates.condition_era_start_date
              - person.observation_period_start_date)
              / 365.25) AS age_bucket,
          osim__condition_count_bucket(person.condition_concepts)
            AS condition_count_bucket,
          osim__procedure_count_bucket(person.procedure_concepts) AS procedure_count_bucket,
          cond_dates.condition_era_start_date AS cond_start_date,
          LEAD (cond_dates.condition_era_start_date,1,person.observation_period_end_date)
            OVER (PARTITION BY cond_dates.person_id
                  ORDER BY cond_dates.condition_era_start_date)
                    AS next_cond_start_date,
          cond_dates.day_cond_count
        FROM v_src_person_strata person
        INNER JOIN
         (SELECT
            person_id,
            condition_era_start_date,
            COUNT(condition_concept_id) AS day_cond_count
          FROM
           (SELECT
              cond.person_id,
              cond.condition_concept_id,
              cond.condition_era_start_date
            FROM v_src_condition_era1 cond
            UNION
            SELECT
              person_id,
              -1 AS condition_concept_id,
              observation_period_start_date AS condition_era_start_date
            FROM v_src_person_strata) t1
          GROUP BY person_id, condition_era_start_date) cond_dates
          ON person.person_id = cond_dates.person_id
        WHERE person.observation_period_end_date
                >= cond_dates.condition_era_start_date) cond_gaps
      LEFT JOIN v_src_first_procedures procedure ON cond_gaps.person_id = procedure.person_id
          AND cond_gaps.cond_start_date <= procedure.procedure_date
          AND cond_gaps.next_cond_start_date > procedure.procedure_date
      GROUP BY cond_gaps.person_id, cond_gaps.gender_concept_id,
       cond_gaps.age_bucket, cond_gaps.condition_count_bucket,
       cond_gaps.procedure_count_bucket,cond_gaps.cond_start_date,
        cond_gaps.next_cond_start_date, cond_gaps.day_cond_count),
      cond_procedure AS
       (SELECT DISTINCT
          gaps.person_id,
          gaps.gender_concept_id,
          gaps.age_bucket,
          gaps.condition_count_bucket,
          gaps.procedure_count_bucket,
          gaps.cond_start_date,
          gaps.next_cond_start_date,
          gaps.day_cond_count,
          coalesce(cond.condition_concept_id,-1) AS condition_concept_id,
          coalesce(procedure.procedure_date,gaps.cond_start_date) AS procedure_occurrence_date,
          coalesce(procedure.procedure_concept_id,-1) AS procedure_concept_id
        FROM gaps
        LEFT JOIN v_src_condition_era1 cond
          ON gaps.person_id = cond.person_id
          AND gaps.cond_start_date = cond.condition_era_start_date
        LEFT JOIN v_src_first_procedures procedure
          ON gaps.person_id = procedure.person_id
            AND gaps.cond_start_date <= procedure.procedure_date
            AND gaps.next_cond_start_date > procedure.procedure_date)
    SELECT
      condition_concept_id,
      interval_bucket,
      gender_concept_id,
      age_bucket,
      condition_count_bucket,
      procedure_count_bucket,
      2 as day_cond_count,
      procedure_concept_id,
      delta_days,
      n,
      SUM(probability)
        OVER
         (PARTITION BY condition_concept_id, interval_bucket, gender_concept_id,
            age_bucket, condition_count_bucket, procedure_count_bucket
          ORDER BY probability DESC
            ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        condition_concept_id,
        interval_bucket,
        gender_concept_id,
        age_bucket,
        condition_count_bucket,
        procedure_count_bucket,
        procedure_concept_id,
        delta_days,
        sum_prob AS n,
        1.0 * sum_prob/ NULLIF(SUM(sum_prob)
                                OVER(PARTITION BY condition_concept_id, interval_bucket,
                    gender_concept_id, age_bucket, condition_count_bucket,
                    procedure_count_bucket), 0) AS probability
      FROM
       (SELECT
          condition_concept_id,
          interval_bucket,
          gender_concept_id,
          age_bucket,
          condition_count_bucket,
          procedure_count_bucket,
          delta_days,
          procedure_concept_id,
          SUM(prob) AS sum_prob
        FROM
         (SELECT
            condition_concept_id,
            osim__duration_days_bucket(next_cond_start_date - cond_start_date)
              AS interval_bucket,
            gender_concept_id,
            age_bucket,
            condition_count_bucket,
            procedure_count_bucket,
            osim__round_days(delta_days) AS delta_days,
            procedure_concept_id,
            1.0 / (day_cond_count) AS prob
          FROM
           (SELECT DISTINCT
              person_id,
              gender_concept_id,
              age_bucket,
              condition_count_bucket,
              procedure_count_bucket,
              cond_start_date,
              next_cond_start_date,
              day_cond_count,
              condition_concept_id,
              procedure_occurrence_date - cond_start_date AS delta_days,
              procedure_concept_id
            FROM cond_procedure
            UNION ALL
            SELECT DISTINCT
              gaps.person_id,
              gaps.gender_concept_id,
              gaps.age_bucket,
              gaps.condition_count_bucket,
              gaps.procedure_count_bucket,
              gaps.cond_start_date,
              gaps.next_cond_start_date,
              gaps.day_cond_count,
              cond.condition_concept_id,
              0 AS delta_days,
              -1 AS procedure_concept_id
            FROM gaps
            INNER JOIN v_src_condition_era1 cond
              ON gaps.person_id = cond.person_id
                AND gaps.cond_start_date = cond.condition_era_start_date
            WHERE gaps.day_cond_count > 1) t1
          ORDER BY 1,2) t2
        GROUP BY condition_concept_id, interval_bucket, gender_concept_id,
              age_bucket, condition_count_bucket, procedure_count_bucket,
              delta_days, procedure_concept_id
        ORDER BY 1,2,5 DESC) t3
     ) t4
    ORDER BY 1,2,6;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_procedure_prob for '
        || 'co-occurent condtion transitions.';
    PERFORM insert_log(MESSAGE, 'ins_cond_procedure_prob');
  raise notice 'Inserted ins_cond_procedure_prob, rows = %', num_rows;

    --COMMIT;


    CREATE INDEX osim_cond_procedure_prob_ix1 ON osim_cond_first_procedure_prob (
      condition_concept_id, interval_bucket, age_bucket, condition_count_bucket,
      procedure_count_bucket, day_cond_count, gender_concept_id)
      WITH (FILLFACTOR = 90);


    CREATE INDEX osim_cond_procedure_prob_ix2
      ON osim_cond_first_procedure_prob (accumulated_probability)
      WITH (FILLFACTOR = 90);

    --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_cond_first_procedure_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition_concept_id, interval_bucket, day_cond_count
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_first_procedure_prob);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_cond_first_procedure_prob');
    raise notice 'Processing complete ins_cond_procedure_prob';
    EXCEPTION
      WHEN OTHERS THEN
      PERFORM insert_log('Exception', 'ins_cond_first_procedure_prob');
      GET STACKED DIAGNOSTICS MESSAGE = PG_EXCEPTION_CONTEXT;
      RAISE NOTICE 'context: >>%<<', MESSAGE;
      raise NOTICE '% %', SQLERRM, SQLSTATE;

  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_drug_era_count_prob()
  /*=========================================================================
  | PROCEDURE ins_drug_era_count_prob
  |
  | Pr(Drug Era Count, Total Exposure | Drug Concept,
  |     Distinct Drug Count Bucket, Distinct Conditon count bucket,
  |     Age Bucket, Time Remaining)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of Drug Eras per Person for a given Drug Concept
  |   based on Drug Concept, Person's Distinct Drug Count Bucket,
  |   Person's Distinct Condition Count Bucket, Person's Age
  |   Bucket (at time of first drug era), and Full Semi-years Remaining
  |   in Person's Observation Period
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows  INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Drug Era Count Analysis', 'ins_drug_era_count_prob');
    TRUNCATE TABLE osim_drug_era_count_prob;

    --COMMIT;
    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_drug_era_count_ix1;
      DROP INDEX osim_drug_era_count_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_drug_era_count_prob');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_drug_era_count_prob
     (drug_concept_id, drug_count_bucket, condition_count_bucket, age_range,
        time_remaining, drug_era_count, total_exposure, n, accumulated_probability)
    SELECT
      drug_concept_id,
      drugs_bucket,
      conds_bucket,
      age_range,
      time_remaining,
      eras,
      total_exposure,
      n,
      SUM(probability) OVER
       (PARTITION BY drug_concept_id, drugs_bucket, conds_bucket,
        age_range, time_remaining
        ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        drug_concept_id,
        drugs_bucket,
        conds_bucket,
		    age_range,
        time_remaining,
        COUNT(person_id) as persons,
        eras,
        total_exposure,
        COUNT(person_id) AS n,
        1.0 * COUNT(person_id)/ NULLIF(SUM(COUNT(person_id)) OVER(PARTITION BY drug_concept_id, drugs_bucket, conds_bucket,
          age_range, time_remaining), 0) AS probability
      FROM
       (SELECT
          drug.drug_concept_id,
          osim__drug_count_bucket(person.drug_concepts) AS drugs_bucket,
          osim__condition_count_bucket(person.condition_concepts) AS conds_bucket,
		      osim__age_bucket(person.age) AS age_range,
          osim__time_observed_bucket(person.observation_period_end_date
            - MIN(drug.drug_era_start_date)) AS time_remaining,
          COUNT(DISTINCT drug.drug_era_id) AS eras,
          osim__round_days(SUM(drug.drug_era_end_date - drug.drug_era_start_date))
            AS total_exposure,
          person.person_id
        FROM v_src_person_strata person
        INNER JOIN v_src_drug_era1 drug ON person.person_id = drug.person_id
        GROUP BY
          drug.drug_concept_id,
          person.drug_concepts,
          person.condition_concepts,
          person.age,
          person.observation_period_end_date,
          person.person_id) t1
      GROUP BY drug_concept_id, eras, total_exposure, drugs_bucket,
        conds_bucket, age_range, time_remaining) t2
    ORDER BY 1,2,3,4,5,9;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_drug_era_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_drug_era_count_prob');
    raise notice 'Inserted ins_drug_era_count_prob, rows = %', num_rows;

    --COMMIT;


      CREATE INDEX osim_drug_era_count_ix1 ON osim_drug_era_count_prob (
        drug_concept_id,
        drug_count_bucket,
        condition_count_bucket,
		    age_range,
        time_remaining)
      WITH (FILLFACTOR = 90);


      CREATE INDEX osim_drug_era_count_ix2 ON osim_drug_era_count_prob (
        accumulated_probability)
      WITH (FILLFACTOR = 90);

    --COMMIT;

    UPDATE osim_drug_era_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY drug_concept_id, drug_count_bucket, condition_count_bucket,
              age_range, time_remaining
            ORDER BY accumulated_probability DESC)
      FROM osim_drug_era_count_prob);

     --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_drug_era_count_prob');
    raise notice 'Processing complete ins_drug_era_count_prob';
    EXCEPTION
      WHEN OTHERS THEN
      PERFORM insert_log('Exception', 'ins_drug_era_count_prob');
    GET STACKED DIAGNOSTICS MESSAGE = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'context: >>%<<', MESSAGE;
    raise notice '% %', SQLERRM, SQLSTATE;
  END;
  $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_procedure_occurrence_count_prob()
  /*=========================================================================
  | PROCEDURE ins_procedure_occurrence_count_prob
  |
  | Pr(procedure_occurrence Count, Total Exposure | procedure Concept,
  |     Distinct procedure Count Bucket, Distinct Conditon count bucket,
  |     Age Bucket, Time Remaining)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of procedure Occurrences per Person for a given procedure Concept
  |   based on procedure Concept, Person's Distinct procedure Count Bucket,
  |   Person's Distinct Condition Count Bucket, Person's Age
  |   Bucket (at time of first procedure occurrence), and Full Semi-years Remaining
  |   in Person's Observation Period
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows  INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting procedure Ocurrence Count Analysis', 'ins_procedure_occurrence_count_prob');
    TRUNCATE TABLE osim_procedure_occurrence_count_prob;

    --COMMIT;
    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_procedure_occurrence_count_ix1;
      DROP INDEX osim_procedure_occurrence_count_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_procedure_occurrence_count_prob');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_procedure_occurrence_count_prob
     (procedure_concept_id, procedure_count_bucket, condition_count_bucket, age_range,
        time_remaining, procedure_occurrence_count, n, accumulated_probability)
    SELECT
      procedure_concept_id,
      procedures_bucket,
      conds_bucket,
      age_range,
      time_remaining,
      eras,
      n,
      SUM(probability) OVER
       (PARTITION BY procedure_concept_id, procedures_bucket, conds_bucket,
        age_range, time_remaining
        ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        procedure_concept_id,
        procedures_bucket,
        conds_bucket,
		    age_range,
        time_remaining,
        COUNT(person_id) as persons,
        eras,
        COUNT(person_id) AS n,
        1.0 * COUNT(person_id)/ NULLIF(SUM(COUNT(person_id)) OVER(PARTITION BY procedure_concept_id, procedures_bucket, conds_bucket,
          age_range, time_remaining), 0) AS probability
      FROM
       (SELECT
          procedure.procedure_concept_id,
          osim__procedure_count_bucket(person.procedure_concepts) AS procedures_bucket,
          osim__condition_count_bucket(person.condition_concepts) AS conds_bucket,
		      osim__age_bucket(person.age) AS age_range,
          osim__time_observed_bucket(person.observation_period_end_date
            - MIN(procedure.procedure_date)) AS time_remaining,
          COUNT(DISTINCT procedure.procedure_occurrence_id) AS eras,
          person.person_id
        FROM v_src_person_strata person
        INNER JOIN v_src_procedure_occurrence1 procedure ON person.person_id = procedure.person_id
        GROUP BY
          procedure.procedure_concept_id,
          person.procedure_concepts,
          person.condition_concepts,
          person.age,
          person.observation_period_end_date,
          person.person_id) t1
      GROUP BY procedure_concept_id, eras, procedures_bucket,
        conds_bucket, age_range, time_remaining) t2
    ORDER BY 1,2,3,4,5,8;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_procedure_occurrence_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_procedure_occurrence_count_prob');
    raise notice 'Inserted ins_procedure_occurrence_count_prob, rows = %', num_rows;

    --COMMIT;


      CREATE INDEX osim_procedure_occurrence_count_ix1 ON osim_procedure_occurrence_count_prob (
        procedure_concept_id,
        procedure_count_bucket,
        condition_count_bucket,
		    age_range,
        time_remaining)
      WITH (FILLFACTOR = 90);


      CREATE INDEX osim_procedure_occurrence_count_ix2 ON osim_procedure_occurrence_count_prob (
        accumulated_probability)
      WITH (FILLFACTOR = 90);

    --COMMIT;

    UPDATE osim_procedure_occurrence_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY procedure_concept_id, procedure_count_bucket, condition_count_bucket,
              age_range, time_remaining
            ORDER BY accumulated_probability DESC)
      FROM osim_procedure_occurrence_count_prob);

     --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_procedure_occurrence_count_prob');
    raise notice 'Processing complete ins_procedure_occurrence_count_prob';
    EXCEPTION
      WHEN OTHERS THEN
      PERFORM insert_log('Exception', 'ins_procedure_occurrence_count_prob');
    GET STACKED DIAGNOSTICS MESSAGE = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'context: >>%<<', MESSAGE;
    raise notice '% %', SQLERRM, SQLSTATE;
  END;
  $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_drug_duration_probability()
  /*=========================================================================
  | PROCEDURE ins_procedure_duration_probability
  |
  | Pr(procedure Duration | Drug Concept, Drug Era Count, Time Remaining)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of days from observation start of first Drug Era to the
  |   end of the last Drug Era
  |   Based on Drug Concept, Drug Era Count, Total Drug Exposure
  |   and Full Semi-years Remaining in the Person's Observation Period
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    num_rows          INTEGER;
    MESSAGE              text;
  BEGIN
    PERFORM insert_log('Starting Drug Concept Duration Probability Analysis',
      'ins_drug_duration_probability');

    TRUNCATE TABLE osim_drug_duration_probability;
    --COMMIT;

    -- Drop Indexes for Quicker Insertion
    BEGIN
      DROP INDEX osim_drug_duration_ix1;
      DROP INDEX osim_drug_duration_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_drug_duration_probability');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_drug_duration_probability
    (drug_concept_id, time_remaining, drug_era_count, total_exposure,
     total_duration, n, accumulated_probability)
    SELECT
      drug_concept_id,
      time_remaining,
      eras,
      total_exposure,
      total_duration,
      n,
      SUM(probability) OVER
       (PARTITION BY drug_concept_id, time_remaining, eras, total_exposure
        ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        drug_concept_id,
        time_remaining,
        eras,
        total_exposure,
        total_duration,
        COUNT(person_id) AS n,
        1.0 * COUNT(person_id)/ NULLIF(SUM(COUNT(person_id)) OVER(PARTITION BY drug_concept_id, time_remaining, eras, total_exposure), 0)
          AS probability
      FROM
       (SELECT
          person.person_id,
          drug.drug_concept_id,
          osim__time_observed_bucket(person.observation_period_end_date - MIN(drug.drug_era_start_date)) AS time_remaining,
          COUNT(drug.drug_era_id) AS eras,
          osim__round_days(SUM(drug.drug_era_end_date - drug.drug_era_start_date)) AS total_exposure,
          osim__round_days(MAX(drug.drug_era_end_date) - MIN(drug.drug_era_start_date))
            AS total_duration
        FROM v_src_person_strata person
        INNER JOIN v_src_drug_era1 drug
          ON person.person_id = drug.person_id
        GROUP BY person.person_id, drug.drug_concept_id, person.observation_period_end_date) t1
      WHERE eras > 1
      GROUP BY drug_concept_id, time_remaining, eras, total_exposure, total_duration) t2
    ORDER BY 1,2,3,4,7;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_drug_duration_probability.';
    PERFORM insert_log(MESSAGE, 'ins_drug_duration_probability');
    raise notice 'Inserted ins_drug_duration_probability, rows %', num_rows;
    --COMMIT;


    CREATE INDEX osim_drug_duration_ix1 ON osim_drug_duration_probability (
      drug_concept_id, time_remaining, drug_era_count, total_exposure)
    WITH (FILLFACTOR = 90);


    CREATE INDEX osim_drug_duration_ix2 ON osim_drug_duration_probability (
      accumulated_probability)
    WITH (FILLFACTOR = 90);

     --COMMIT;

    -- a few of the last buckets may not quite add up to 1.0
    UPDATE osim_drug_duration_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY drug_concept_id, time_remaining,
              drug_era_count, total_exposure
            ORDER BY accumulated_probability DESC)
      FROM osim_drug_duration_probability);

    --COMMIT;

    PERFORM insert_log('Processing complete', 'ins_drug_duration_probability');
    raise notice 'Processing complete ins_drug_duration_probability';

    EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_drug_duration_probability');
    GET STACKED DIAGNOSTICS MESSAGE = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'context: >>%<<', MESSAGE;
    raise notice '% %', SQLERRM, SQLSTATE;
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION drop_osim_indexes()
  /*=========================================================================
  | PROCEDURE drop_osim_indexes
  |
  | Drop all OSIM table indexes for quicker insertion
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE

  BEGIN
    PERFORM insert_log('Dropping OSIM indexes for quicker insertion',
      'drop_osim_indexes');

    BEGIN
      DROP INDEX xn_procedure_occurrence_concept_id;
      DROP INDEX xn_procedure_occurrence_person_id;
      DROP INDEX xn_procedure_occurrence_start_date;
      DROP INDEX xn_cond_era_concept_id;
      DROP INDEX xn_cond_era_person_id;
      DROP INDEX xn_cond_era_start_date;
      DROP INDEX xn_drug_era_concept_id;
      DROP INDEX xn_drug_era_person_id;
      DROP INDEX xn_drug_era_start_date;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Simulated data indexes are already removed',
            'drop_osim_indexes');
    END;

    PERFORM insert_log('Processing complete', 'drop_osim_indexes');

  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_osim_indexes()
  /*=========================================================================
  | PROCEDURE create_osim_indexes
  |
  | Create all OSIM table indexes for quicker selection
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
  BEGIN
    PERFORM insert_log('Creating OSIM indexes for quicker selection',
      'create_osim_indexes');

    BEGIN

      CREATE INDEX xn_cond_era_concept_id
        ON osim_condition_era (condition_concept_id ASC)
      WITH (FILLFACTOR = 90);


      CREATE INDEX xn_cond_era_person_id ON osim_condition_era (person_id ASC)
      WITH (FILLFACTOR = 90);


      CREATE INDEX xn_cond_era_start_date
        ON osim_condition_era (condition_era_start_date ASC)
      WITH (FILLFACTOR = 90);


      CREATE INDEX xn_drug_era_concept_id ON osim_drug_era (drug_concept_id ASC)
      WITH (FILLFACTOR = 90);


      CREATE INDEX xn_drug_era_person_id ON osim_drug_era (person_id ASC)
      WITH (FILLFACTOR = 90);


      CREATE INDEX xn_drug_era_start_date ON osim_drug_era (drug_era_start_date ASC)
      WITH (FILLFACTOR = 90);

    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Simulated data indexes already exist',
            'create_osim_indexes');
    END;

    PERFORM insert_log('Processing complete', 'create_osim_indexes');
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION copy_final_data (
  /*=========================================================================
  | PROCEDURE copy_final_data
  |
  | Copy simulate data to final result tables
  |
  |==========================================================================
  */
    destination_schema   VARCHAR DEFAULT 'osim_dev',
    total_person_count   INTEGER   DEFAULT 5000
  )
  RETURNS VOID AS $$
  DECLARE
    current_person_count INTEGER;
--     tmp_max_id           INTEGER;
    tmp_sql              VARCHAR(2000);
  BEGIN
    SELECT COUNT(*)
    INTO current_person_count
    FROM osim_person;

    IF current_person_count = total_person_count THEN
      SELECT create_osim_indexes();

      PERFORM insert_log('Copying person data', 'copy_final_data');

      tmp_sql :=
       'INSERT /*+ APPEND NOLOGGING */ INTO ' ||
         destination_schema || '.person ' ||
       ' (person_id,year_of_birth,gender_concept_id,race_concept_id) ' ||
       'SELECT person_id, year_of_birth, gender_concept_id, NULL AS race_concept_id ' ||
       'FROM osim_person ' ||
       'ORDER BY person_id';

      PERFORM tmp_sql;
      --COMMIT;

      PERFORM insert_log('Copying observation period data', 'copy_final_data');

      tmp_sql :=
       'INSERT /*+ APPEND NOLOGGING */ INTO ' ||
          destination_schema || '.observation_period ' ||
       '  (observation_period_id, person_id, observation_period_start_date, ' ||
       '   observation_period_end_date, rx_data_availability, dx_data_availability, ' ||
       '   hospital_data_availability) ' ||
       'SELECT observation_period_id, person_id, observation_period_start_date, ' ||
       '  observation_period_end_date, rx_data_availability, dx_data_availability, ' ||
       '  hospital_data_availability ' ||
       'FROM osim_observation_period ' ||
       'ORDER BY person_id';

      PERFORM tmp_sql;
      --COMMIT;

      PERFORM insert_log('Copying condition era data', 'copy_final_data');

      tmp_sql :=
       'INSERT /*+ APPEND NOLOGGING */ INTO ' ||
          destination_schema || '.omop_condition_era ' ||
       '  (condition_era_id, condition_era_start_date, condition_era_end_date, ' ||
       '   person_id, condition_concept_id, ' ||
       '   condition_occurrence_count) ' ||
       'SELECT ROWNUM, condition_era_start_date, condition_era_end_date, ' ||
       '  person_id, condition_concept_id, ' ||
       '  condition_occurrence_count ' ||
       'FROM osim_condition_era ' ||
       'ORDER BY condition_era_start_date, person_id';

      PERFORM tmp_sql;
      --COMMIT;

      PERFORM insert_log('Copying drug era data', 'copy_final_data');

      tmp_sql :=
       'INSERT /*+ APPEND NOLOGGING */ INTO ' ||
          destination_schema || '.omop_drug_era ' ||
       '  (drug_era_id, drug_era_start_date, drug_era_end_date, person_id, ' ||
       '   drug_exposure_type, drug_concept_id, drug_exposure_count) ' ||
       'SELECT ROWNUM, drug_era_start_date, drug_era_end_date, person_id, ' ||
       '  drug_exposure_type, drug_concept_id, drug_exposure_count ' ||
       'FROM osim_drug_era ' ||
       'ORDER BY drug_era_start_date, person_id';

      PERFORM tmp_sql;
      --COMMIT;

      PERFORM insert_log('Data copying complete', 'copy_final_data');

    ELSE
      PERFORM insert_log('All data is not complete for final copy', 'copy_final_data');
    END IF;

    PERFORM insert_log('Processing complete', 'copy_final_data');
  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'copy_final_data');

  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_cond_era_count_prob()
  /*=========================================================================
  | PROCEDURE ins_cond_era_count_prob
  |
  | Pr(Condition Era Count | Condition Concept,
  |     Distinct Condition Count Bucket, Time Remaining)
  |
  | Analyze source CDM database and store results for:
  |   INTEGER of Condition Eras per Person for a given Condition Concept
  |   Based on Condition Condition, Distinct Condition Concept Count Bucket,
  |     and Full Semi-years Remaining in Person's Observation Period
  |
  |==========================================================================
  */ RETURNS VOID AS $$
  DECLARE
    MESSAGE               TEXT;
    num_rows              INTEGER;
  BEGIN
    PERFORM insert_log('Starting Condition Era Count Analysis', 'ins_cond_era_count_prob');
     TRUNCATE TABLE osim_cond_era_count_prob;
    --COMMIT;

    -- Drop Indexes for Quicker Insertion
    BEGIN
       DROP INDEX osim_cond_era_count_ix1;
       DROP INDEX osim_cond_era_count_ix2;
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_cond_era_count_prob');
    END;

    --COMMIT;

    INSERT /*+ append nologging */ INTO osim_cond_era_count_prob
     (condition_concept_id, cond_count_bucket, time_remaining, cond_era_count,
      n, accumulated_probability)
    SELECT
      condition_concept_id,
      conds_bucket,
      time_remaining,
      eras,
      persons AS n,
      SUM(probability) OVER
       (PARTITION BY condition_concept_id, conds_bucket, time_remaining
        ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        condition_concept_id,
        conds_bucket,
        time_remaining,
        COUNT(person_id) as persons,
        eras,
        1.0 * COUNT(person_id) / NULLIF(SUM(COUNT(person_id)) OVER(PARTITION BY condition_concept_id, conds_bucket, time_remaining),0) AS probability
      FROM
       (SELECT
          cond.condition_concept_id,
          osim__condition_count_bucket(person.condition_concepts) AS conds_bucket,
          osim__time_observed_bucket(person.observation_period_end_date
            - MIN(cond.condition_era_start_date)) AS time_remaining,
          COUNT(DISTINCT cond.condition_era_id) AS eras,
          person.person_id
        FROM v_src_person_strata person
        INNER JOIN v_src_condition_era1 cond ON person.person_id = cond.person_id
        GROUP BY
          cond.condition_concept_id,
          person.condition_concepts,
          person.observation_period_end_date,
          person.person_id) t1
      GROUP BY condition_concept_id, eras, conds_bucket, time_remaining) t2
    ORDER BY 1,2,3,6;

    --COMMIT;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_era_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_cond_era_count_prob');


      CREATE INDEX osim_cond_era_count_ix1 ON osim_cond_era_count_prob (
        condition_concept_id,
        cond_count_bucket,
        time_remaining)
      WITH (FILLFACTOR = 90);


      CREATE INDEX osim_cond_era_count_ix2 ON osim_cond_era_count_prob (
        accumulated_probability)
      WITH (FILLFACTOR = 90);

    --COMMIT;

    UPDATE osim_cond_era_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition_concept_id, cond_count_bucket, time_remaining
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_era_count_prob);

    --COMMIT;
    PERFORM insert_log('Processing complete', 'ins_cond_era_count_prob');

  END;
  $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analyze_source_db()
  /*=========================================================================
  | PROCEDURE analyze_source_db
  |
  | Execute all procedures to analyze CDM data and repopulate the
  | probability and transition tables.
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
  BEGIN
    PERFORM insert_log('Starting Condition Concept Reoccurrence Probability Analysis',
        'analyze_source_db');

    PERFORM ins_src_db_attributes();
    -- For person generation
    PERFORM ins_gender_probability();
    PERFORM ins_age_at_obs_probability();

    -- For observation period generation
    PERFORM ins_cond_count_probability();
    PERFORM ins_time_obs_probability();

    -- For condition generation
    PERFORM ins_cond_era_count_prob();
    PERFORM ins_first_cond_probability();
    PERFORM ins_cond_days_before_prob();

    -- For drug generation
    PERFORM ins_drug_count_prob();
    PERFORM ins_cond_drug_count_prob();
    PERFORM ins_cond_first_drug_prob();
    PERFORM ins_drug_era_count_prob();
    PERFORM ins_drug_duration_probability();

    -- For procedure generation
    PERFORM ins_procedure_count_prob();
      PERFORM ins_cond_procedure_count_prob();
    PERFORM ins_cond_first_procedure_prob();
    PERFORM ins_procedure_occurrence_count_prob();
    PERFORM ins_procedure_days_before_prob();

    PERFORM insert_log('Processing complete', 'analyze_source_db');

  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_all_sim_data()
  /*=========================================================================
  | PROCEDURE delete_all_sim_data
  |
  | Truncate all simulated date tables.
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
  BEGIN
    PERFORM insert_log('Deleting all simulate data', 'delete_all_sim_data');

    TRUNCATE TABLE osim_person CASCADE;
    TRUNCATE TABLE osim_observation_period CASCADE;
    TRUNCATE TABLE osim_condition_era CASCADE;
    TRUNCATE TABLE osim_drug_era CASCADE;
    TRUNCATE TABLE osim_procedure_occurrence CASCADE;
    --COMMIT;


    PERFORM insert_log('Processing complete', 'delete_all_sim_data');
  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'delete_all_sim_data');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sim_initialization (
  /*=========================================================================
  | PROCEDURE SIM_INITIALIZATION
  |
  |  Simulation Initialization
  |==========================================================================
  */
    person_start_id                   INTEGER,
    OUT my_sid                        VARCHAR,
    OUT my_start_time                 DATE,
    OUT db_min_date                   DATE,
    OUT db_max_date                   DATE,
    OUT db_min_year                   INTEGER,
    OUT db_max_year                   INTEGER,
    OUT max_person_id                 INTEGER,
    OUT max_condition_era_id          INTEGER,
    OUT max_drug_era_id               INTEGER,
    OUT max_procedure_occurrence_id   INTEGER,
    OUT osim_db_persons               INTEGER,
    OUT drug_persistence              INTEGER,
    OUT cond_persistence              INTEGER
  )
  RETURNS RECORD AS $$
  DECLARE
  BEGIN
    PERFORM insert_log('Initializing Simulation',
      'sim_initialization');

    SELECT session_user
    INTO my_sid;

    SELECT current_timestamp
    INTO my_start_time;

    PERFORM insert_log('My SID=' || my_sid || ', Simulation Start Time=' ||
      to_char(my_start_time, 'Dy DD-Mon-YYYY HH24:MI:SS'),
      'sim_initialization');

    -- Get Min and Max dates for the simulation date range
    SELECT src.db_min_date, src.db_max_date
    FROM osim_src_db_attributes src
    LIMIT 1 INTO db_min_date, db_max_date;

    db_min_year := CAST(extract(year from db_min_date) AS INTEGER);
    db_max_year := CAST(extract(year from db_max_date) AS INTEGER);

    -- Get max person_id from existing simulated data
    -- to maintain unique ID generation
    IF person_start_id = 0 THEN
      SELECT coalesce(MAX(person_id),0)
      INTO max_person_id
      FROM osim_person;
    ELSE
      max_person_id := person_start_id - 1;
    END IF;

    -- Get max condition_era_id from existing simulated data
    -- to maintain unique ID generation
    SELECT coalesce(MAX(condition_era_id),0)
    INTO max_condition_era_id
    FROM osim_condition_era;

    -- Get max drug_era_id from existing simulated data
    -- to maintain unique ID generation
    SELECT coalesce(MAX(drug_era_id),0)
    INTO max_drug_era_id
    FROM osim_drug_era;

    --Get max_procedure_occurrence_id from existsing simulated data
    --to maintain unique ID generation
    SELECT coalesce(MAX(procedure_occurrence_id),0)
    INTO max_procedure_occurrence_id
    FROM osim_procedure_occurrence;

    -- Get current person count in existing simulated data
    SELECT COUNT(distinct person_id)
    INTO osim_db_persons
    FROM osim_person;

    PERFORM insert_log('Processing complete', 'sim_initialization');
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_sim_person (
  /*=========================================================================
  | PROCEDURE INS_SIM_PERSON
  |
  |  Simulate Person
  |==========================================================================
  */
    db_min_year                   INTEGER,
    INOUT max_person_id           INTEGER,
    OUT this_gender               INTEGER,
    OUT this_age                  INTEGER,
    OUT this_age_bucket           INTEGER,
    OUT this_year_of_birth        INTEGER
  )
  RETURNS RECORD AS $$
  DECLARE
    tmp_rand                      FLOAT;
    i                             INTEGER;
  BEGIN
    --
    -- Create osim person
    --
    -- Try 5 times to generate a person before giving nulls

    BEGIN
      -- Draw for gender_concept_id
      tmp_rand := random();
      SELECT DISTINCT FIRST_VALUE(gender_concept_id)
        OVER (ORDER BY accumulated_probability)
      INTO STRICT this_gender
      FROM osim_gender_probability
      WHERE tmp_rand <= accumulated_probability;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          PERFORM insert_log('Null Value in gender', 'ins_sim_person');
    END;

    BEGIN
        -- Draw for age
      tmp_rand := random();
      SELECT DISTINCT FIRST_VALUE(age_at_obs)
        OVER (ORDER BY accumulated_probability)
      INTO STRICT this_age
      FROM osim_age_at_obs_probability
      WHERE gender_concept_id = this_gender
        AND tmp_rand <= accumulated_probability;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          PERFORM insert_log('Null value in age', 'ins_sim_person');
    END;

    -- Calculate this person' age bucket and year of birth
    this_age_bucket := osim__age_bucket(this_age);
    this_year_of_birth := db_min_year - this_age;
    max_person_id := max_person_id + 1;
--     PERFORM insert_log('Simulated a person',
--         'ins_sim_person');
  END;
 $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_sim_observation_period (
  /*=========================================================================
  | PROCEDURE INS_SIM_OBSERVATION_PERIOD
  |
  |  Simulate Person Observation Period
  |==========================================================================
  */
    max_person_id                 INTEGER,
    this_gender                   INTEGER,
    this_age                      INTEGER,
    db_min_date                   DATE,
    db_max_date                   DATE,
    INOUT this_year_of_birth      INTEGER,
    OUT this_cond_count_limit     INTEGER,
    OUT this_cond_count_bucket    INTEGER,
    OUT this_obs_bucket           INTEGER,
    OUT this_obs_days             INTEGER,
    OUT this_person_begin_date    DATE,
    OUT this_person_end_date      DATE
  )
  RETURNS RECORD AS $$
  DECLARE
    tmp_rand                      FLOAT;
    tmp                           INTEGER;
  BEGIN
    --
    -- Create Observation Period
    --

    -- Draw for distinct condition count
    tmp_rand := random();
    SELECT DISTINCT FIRST_VALUE(cond_concept_count)
      OVER (ORDER BY accumulated_probability)
    INTO this_cond_count_limit
    --select count(*)
    FROM osim_cond_count_probability
    WHERE gender_concept_id = this_gender
      AND age_at_obs = this_age
      AND tmp_rand <= accumulated_probability;

    this_cond_count_bucket := osim__condition_count_bucket(this_cond_count_limit);

    -- Draw for full semi-years observed (observation period duration)
    tmp_rand := random();
    SELECT DISTINCT FIRST_VALUE(time_observed)
      OVER (ORDER BY accumulated_probability)
    INTO this_obs_bucket
    FROM osim_time_obs_probability
    WHERE gender_concept_id = this_gender
      AND age_at_obs = this_age
      AND cond_count_bucket = this_cond_count_bucket
      AND tmp_rand <= accumulated_probability;

    this_obs_days := FLOOR((random() * 0.9999 + this_obs_bucket) * 182.625);

    -- Randomly assign person start date
    -- Is this an issue for new and discontinued drug concepts?
    --

    tmp := FLOOR(((db_max_date - this_obs_days) - db_min_date) * random());
    this_person_begin_date := db_min_date;
    SELECT tmp + db_min_date INTO this_person_begin_date;

    this_year_of_birth := this_year_of_birth + CAST(extract(year from this_person_begin_date) AS INTEGER)
      - CAST(extract(year from db_min_date) AS INTEGER);

    -- Insert person
    INSERT INTO osim_person
    (person_id,year_of_birth,gender_concept_id,race_concept_id)
    SELECT
      max_person_id AS person_id,
      this_year_of_birth AS year_of_birth,
      this_gender AS gender_concept_id,
      NULL AS race_concept_id
    ;

    -- Create observation period record
    this_person_end_date := this_person_begin_date + this_obs_days;

    INSERT INTO osim_observation_period
    (observation_period_id, person_id, observation_period_start_date,
      observation_period_end_date, rx_data_availability, dx_data_availability,
     hospital_data_availability
    )
    SELECT
      max_person_id AS observation_period_id,
      max_person_id AS person_id,
      this_person_begin_date AS observation_period_start_date,
      this_person_end_date AS observation_period_end_date,
      'Y', 'Y', 'N'
    ;
  END;
$$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION ins_sim_conditions (
  /*=========================================================================
  | PROCEDURE INS_SIM_CONDITIONS
  |
  |  Simulate Condition Eras
  |==========================================================================
  */
    max_person_id                       INTEGER,
    this_person_begin_date              DATE,
    this_age                            INTEGER,
    this_gender                         INTEGER,
    this_obs_days                       INTEGER,
    INOUT this_person_cond_count_limit  INTEGER,
    INOUT max_condition_era_id            INTEGER,
    INOUT this_cond_count_bucket          INTEGER
  )
  RETURNS RECORD AS $$
  DECLARE
    this_person_conditions        INTEGER ARRAY;
    tmp_rand                      FLOAT;
    this_person_cond_count        INTEGER;
    this_person_era_count         INTEGER;
    this_cond_date                DATE;
    this_cond_concept             INTEGER;
    this_cond_delta_days          INTEGER;
    init_cond_date                DATE;
    init_cond_concept             INTEGER;
    init_cond_delta_days          INTEGER;
    init_cond_age                 INTEGER;
    init_cond_time_remaining      INTEGER;
    this_cond_age                 INTEGER;
    this_cond_age_bucket          INTEGER;
    this_cond_time_remaining      INTEGER;
    tmp_time_remaining            INTEGER;
    prev_cond_concept             INTEGER;
    this_era_date                 DATE;
    this_end_date                 DATE;
    this_era_age                  INTEGER;
    this_era_age_bucket           INTEGER;
    this_era_delta_days           INTEGER;
    this_era_time_remaining       INTEGER;
    this_cond_era_count_limit     INTEGER;
    this_cond_era_count           INTEGER;

  BEGIN
    -- Clear existing condition concepts for new person

--     TRUNCATE TABLE this_person_conditions_table;
    this_person_conditions := '{}';

    this_person_cond_count := 0;
    this_person_era_count := 0;

    -- Initial date and condition concept of -1
    --
    this_cond_date := this_person_begin_date;
    this_cond_concept := -1;
    this_cond_delta_days := 0;

    init_cond_date := this_person_begin_date;
    init_cond_concept := -1;
    init_cond_delta_days := 0;

    --
    -- Simulate Conditions
    --
    WHILE this_person_cond_count < this_person_cond_count_limit
    LOOP

      -- For very first condition, non bucketed age is used
      -- For subsequent conditions based on prior condition transition,
      --   the age bucket is used
      this_cond_age :=
        CASE
          WHEN this_cond_concept = -1 THEN this_age
          ELSE this_cond_age + this_cond_delta_days / 365.25
        END;

      this_cond_age_bucket :=
        CASE
          WHEN this_cond_concept = -1 THEN this_cond_age
          ELSE osim__age_bucket(CAST(this_cond_age AS INTEGER))
        END;

      this_cond_date :=
        CASE
          WHEN this_cond_concept = -1 THEN this_person_begin_date
          ELSE this_cond_date + this_cond_delta_days
        END;

      this_cond_time_remaining :=
        CASE
          WHEN this_cond_concept = -1 THEN this_obs_days
          ELSE this_cond_time_remaining - this_cond_delta_days
        END;

      tmp_time_remaining := osim__time_observed_bucket(this_cond_time_remaining);

      -- Draw for next condition and days until it starts
      prev_cond_concept := this_cond_concept;
      tmp_rand := random();
      BEGIN
        SELECT DISTINCT
          FIRST_VALUE(condition2_concept_id)
            OVER (ORDER BY accumulated_probability),
          coalesce(FIRST_VALUE(delta_days)
            OVER (ORDER BY accumulated_probability) ,0)
        INTO STRICT this_cond_concept, this_cond_delta_days
          FROM osim_first_cond_probability prob
          WHERE prob.condition1_concept_id = this_cond_concept
            AND prob.gender_concept_id = this_gender
            AND prob.age_range = this_cond_age_bucket
            AND prob.cond_count_bucket = this_cond_count_bucket
            AND time_remaining = tmp_time_remaining
            AND tmp_rand <= prob.accumulated_probability;
      EXCEPTION
        WHEN NO_DATA_FOUND
          THEN
            this_cond_concept := -1;
            this_cond_delta_days := 0;
      END;

      -- Randomize from returned days bucket
      this_cond_delta_days := (osim__randomize_days(this_cond_delta_days));

      -- If the person already has this condition concept,
      --   do not add it again

      IF this_cond_concept >= 0
        AND NOT EXISTS (SELECT this_cond_concept = ANY(this_person_conditions))
-- AND NOT EXISTS (SELECT 1 FROM this_person_conditions_table WHERE condition_concept_id = this_cond_concept)
      THEN

        this_era_date := this_cond_date + this_cond_delta_days;
        this_era_age := this_cond_age + this_cond_delta_days / 365.25;
        this_era_time_remaining := this_cond_time_remaining - this_cond_delta_days;

        IF init_cond_concept = -1 THEN
          init_cond_concept := this_cond_concept;
          init_cond_age := this_era_age;
          init_cond_time_remaining := this_era_time_remaining;
          init_cond_date := this_era_date;
        END IF;

        -- Draw For Conditon Era Count for this Condition Concept
        tmp_rand := random();
        tmp_time_remaining := osim__time_observed_bucket(this_era_time_remaining);
        BEGIN
          SELECT DISTINCT
            FIRST_VALUE(cond_era_count) OVER (ORDER BY accumulated_probability)
          INTO STRICT this_cond_era_count_limit
          FROM osim_cond_era_count_prob prob
          WHERE prob.cond_count_bucket = this_cond_count_bucket
            AND prob.condition_concept_id = this_cond_concept
            AND prob.time_remaining = tmp_time_remaining
            AND tmp_rand <= prob.accumulated_probability;
        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              -- this should not ever happen
              this_cond_era_count_limit := 1;
        END;

        -- Write first Condition Era  for the Conditon Concept
        max_condition_era_id := max_condition_era_id + 1;
        INSERT INTO osim_tmp_condition_era
         (condition_era_id, condition_era_start_date, condition_era_end_date,
          person_id, condition_concept_id,
          condition_occurrence_count)
          SELECT
            max_condition_era_id,
            this_era_date,
            this_era_date,
            max_person_id,
            this_cond_concept,
            1
          ;
        this_person_era_count := this_person_era_count + 1;
        this_cond_era_count := 1;

        this_person_cond_count := this_person_cond_count + 1;

       INSERT INTO this_person_conditions VALUES (this_cond_concept);

        -- Now Insert Condition Reoccurrences
        WHILE this_cond_era_count < this_cond_era_count_limit
        LOOP
          tmp_rand := random();
          tmp_time_remaining := osim__time_observed_bucket(this_era_time_remaining);
          this_era_age_bucket := osim__age_bucket(this_era_age);

          -- Draw for days until the subsequent Conditon Era
          BEGIN
            SELECT DISTINCT
              FIRST_VALUE(delta_days)
                OVER (ORDER BY accumulated_probability) as delta_days
            INTO STRICT this_era_delta_days
            FROM osim_cond_reoccur_probability prob
              WHERE condition_concept_id = this_cond_concept
              AND age_range = this_era_age_bucket
              AND time_remaining = tmp_time_remaining
              AND tmp_rand <= accumulated_probability;
          EXCEPTION
            WHEN NO_DATA_FOUND
              THEN
                -- Reset to first occurrence
                this_era_age_bucket := 0;
                this_era_date := this_cond_date + this_cond_delta_days;
                this_era_age := this_cond_age + this_cond_delta_days / 365.25;
                this_era_time_remaining := this_cond_time_remaining
                    - this_cond_delta_days;
                this_cond_era_count_limit := this_cond_era_count_limit - 1;
          END;
          IF this_era_age_bucket > 0 THEN
            --
            -- Write Condition Era
            --
            -- Randomize from returned days bucket
            this_era_delta_days := (osim__randomize_days(this_era_delta_days));
            this_era_date := this_era_date + this_era_delta_days;
            this_era_age := this_era_age + this_era_delta_days / 365.25;
            this_era_time_remaining := this_era_time_remaining
                - this_era_delta_days;

            max_condition_era_id := max_condition_era_id + 1;
            INSERT INTO osim_tmp_condition_era
             (condition_era_id, condition_era_start_date, condition_era_end_date,
              person_id, condition_concept_id,
              condition_occurrence_count)
              SELECT
                max_condition_era_id,
                this_era_date,
                this_era_date,
                max_person_id,
                this_cond_concept,
                1
              ;

            this_person_era_count := this_person_era_count + 1;
            this_cond_era_count := this_cond_era_count + 1;

          END IF;

          IF this_era_time_remaining < 0 THEN
            this_era_date := this_cond_date;
            this_era_age := this_cond_age;
            this_era_time_remaining := this_cond_time_remaining;
            this_cond_era_count_limit := this_cond_era_count_limit - 1;
          END IF;

        END LOOP;

      ELSE -- person already has drawn conditon

        IF this_cond_concept < 0 AND init_cond_concept < 0 THEN
          this_person_cond_count_limit := this_person_cond_count_limit - 1;
        END IF;

        IF this_cond_concept < 0 AND init_cond_concept >= 0 THEN
          IF random() <= 0.5 THEN
            this_cond_concept := init_cond_concept;
            this_cond_age := init_cond_age;
            this_cond_time_remaining := init_cond_time_remaining;
            this_cond_date := init_cond_date;
            --Prevent Deadlock
            IF random() <= 0.2 THEN
              this_person_cond_count_limit := this_person_cond_count_limit - 1;
            END IF;
          END IF;
          this_cond_delta_days := 0;
        END IF;

        IF this_cond_concept >= 0 THEN
          IF random() <= 0.5 THEN
            this_cond_delta_days := 0;
          END IF;

        END IF;

      END IF;

    END LOOP; -- All Conditon Eras are complete for this person

    -- Just in case there are duplicate conditions on the same date
    DELETE FROM osim_tmp_condition_era
    WHERE condition_era_id
    IN
     (SELECT cond1.condition_era_id
      FROM osim_tmp_condition_era cond1
      INNER JOIN osim_tmp_condition_era cond2
        ON cond1.person_id = cond2.person_id
          AND cond1.condition_era_start_date = cond2.condition_era_start_date
          AND cond1.condition_concept_id = cond2.condition_concept_id
      WHERE cond1.condition_era_id > cond2.condition_era_id);

    -- Just in case there are conditions out of observation date
    this_end_date = this_person_begin_date + this_obs_days;
    DELETE FROM osim_tmp_condition_era
    WHERE condition_era_start_date >= this_end_date;

    -- Set actual Condition Count Bucket
    this_person_cond_count_limit := this_person_cond_count;
    this_cond_count_bucket := osim__condition_count_bucket(this_person_cond_count);

--     PERFORM insert_log('Simulated condition',
--         'ins_sim_condition');
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION ins_sim_drugs (
  /*=========================================================================
  | PROCEDURE INS_SIM_DRUGS
  |
  |  Simulate Drug Eras
  |==========================================================================
  */
    max_person_id                 INTEGER,
    this_person_begin_date        DATE,
    INOUT max_drug_era_id            INTEGER,
    this_person_end_date          DATE,
    this_gender                   INTEGER,
    this_cond_count_bucket        INTEGER,
    this_age                      INTEGER,
    this_age_bucket               INTEGER,
    drug_persistence              INTEGER

  )
  RETURNS INTEGER AS $$
  DECLARE
    this_person_drugs             INTEGER ARRAY;
    tmp_rand                      FLOAT;
    this_target_drug_count        INTEGER;
    this_target_drug_bucket       INTEGER;
    this_person_drug_count        INTEGER;
    this_cond_drug_count          INTEGER;
    this_drug_concept             INTEGER;
    this_drug_delta_days          INTEGER;

  BEGIN
    --
    -- Simulate Drugs
    --   Drugs are simulated from the person's Conditons

    -- Clear existing condition and drug concepts for new person


-- TRUNCATE TABLE this_person_drugs_table;
    this_person_drugs := '{}';

    -- Draw for person's Distinct Drug Concept count
    BEGIN
      tmp_rand := random();
      SELECT DISTINCT FIRST_VALUE(drug_count)
        OVER (ORDER BY accumulated_probability)
      INTO STRICT this_target_drug_count
      FROM osim_drug_count_prob
      WHERE gender_concept_id = this_gender
        AND age_bucket = this_age_bucket
        AND condition_count_bucket = this_cond_count_bucket
        AND tmp_rand <= accumulated_probability;
    EXCEPTION
      WHEN NO_DATA_FOUND
        THEN
          this_target_drug_count := 0;
    END;
    this_target_drug_bucket := osim__drug_count_bucket(this_target_drug_count);

    this_person_drug_count := 0;

    --
    -- Begin Drug simulation Loop
    --
    WHILE this_person_drug_count < this_target_drug_count
    LOOP
      DECLARE
        tmp_age                   INTEGER;
        tmp_age_bucket            INTEGER;
        -- Cursor of all condition eras and starta needed for transitions
        cond_era_cur CURSOR FOR
          SELECT
            person_id,
            condition_concept_id,
            condition_era_start_date,
            interval_bucket,
            day_cond_count
          FROM
           (SELECT DISTINCT
              cond_lag.person_id,
              coalesce(cond.condition_concept_id,-1) AS condition_concept_id,
              cond_lag.lag_start_date AS condition_era_start_date,
              osim__duration_days_bucket(cond_lag.lag_end_date
                  - cond_lag.lag_start_date) AS interval_bucket,
              cond_lag.day_cond_count
            FROM
             (SELECT
                cond_dates.person_id,
                cond_dates.condition_era_start_date AS lag_start_date,
                LEAD (cond_dates.condition_era_start_date,1,
                  cond_dates.condition_era_start_date+30)
                  OVER (PARTITION BY cond_dates.person_id
                        ORDER BY cond_dates.condition_era_start_date)
                          AS lag_end_date,
                cond_dates.day_cond_count
              FROM
               (SELECT
                  cond.person_id,
                  cond.condition_era_start_date,
                  COUNT (DISTINCT cond.condition_concept_id) AS day_cond_count
                FROM osim_tmp_condition_era cond
                WHERE cond.person_id=max_person_id
                GROUP BY cond.person_id, cond.condition_era_start_date
                UNION
                SELECT person_id, condition_era_start_date, 0
                FROM osim_tmp_condition_era cond
                  WHERE cond.person_id = max_person_id
                  AND cond.condition_era_start_date = this_person_begin_date
                AND cond.condition_era_id IS NULL) cond_dates) cond_lag
            LEFT JOIN osim_tmp_condition_era cond
              ON cond_lag.person_id = cond.person_id
                AND cond_lag.lag_start_date=cond.condition_era_start_date) t1
          ORDER BY 5 DESC;

      BEGIN
        <<cond_loop>>
        FOR cond_era IN cond_era_cur LOOP
          -- Draw for INTEGER of Drug Draws
          BEGIN
            tmp_age := (cond_era.condition_era_start_date
              - this_person_begin_date) / 365.25 + this_age;
            tmp_age_bucket := osim__age_bucket(tmp_age);

            tmp_rand := random();

            SELECT DISTINCT
              FIRST_VALUE(drug_count)
                OVER (ORDER BY count_prob.accumulated_probability) AS drug_era_count
            INTO STRICT this_cond_drug_count
            FROM osim_cond_drug_count_prob count_prob
            WHERE count_prob.condition_concept_id = cond_era.condition_concept_id
              AND count_prob.drug_count_bucket = this_target_drug_bucket
              AND count_prob.condition_count_bucket = this_cond_count_bucket
              AND count_prob.interval_bucket = cond_era.interval_bucket
              AND count_prob.age_bucket = tmp_age_bucket
              AND tmp_rand <= count_prob.accumulated_probability;
          EXCEPTION
            WHEN NO_DATA_FOUND
              THEN
                this_cond_drug_count := 0;
          END;

          -- Force the last few Drugs, if necessary
          IF this_target_drug_count - this_person_drug_count < 10
             AND this_cond_drug_count = 0 THEN
            this_cond_drug_count := 1;
          END IF;


          FOR i IN 1..this_cond_drug_count
          LOOP
          -- Draw for Drug Concept
            BEGIN
              tmp_rand := random();
              SELECT DISTINCT
                FIRST_VALUE(drug_concept_id)
                  OVER (ORDER BY accumulated_probability),
                coalesce(FIRST_VALUE(delta_days)
                  OVER (ORDER BY accumulated_probability),0)
              INTO STRICT this_drug_concept, this_drug_delta_days
              FROM osim_cond_first_drug_prob prob
              WHERE prob.condition_concept_id = cond_era.condition_concept_id
                AND prob.drug_count_bucket = this_target_drug_bucket
                AND prob.condition_count_bucket = this_cond_count_bucket
                AND prob.gender_concept_id = this_gender
                AND prob.age_bucket = this_age_bucket
                AND prob.day_cond_count = 2
                  --CASE WHEN cond_era.day_cond_count > 1 THEN 2 ELSE 1 END
                AND tmp_rand <= prob.accumulated_probability;
            EXCEPTION
              WHEN NO_DATA_FOUND
                THEN
                  this_drug_concept := 0;
                  this_drug_delta_days := 0;
            END;

            IF this_drug_concept > 0
                AND NOT EXISTS (SELECT this_drug_concept = ANY(this_person_drug)) THEN
               --AND NOT EXISTS (select 1 from this_person_drugs_table where drug_concept_id = this_drug_concept) THEN
              this_drug_delta_days := (osim__randomize_days(this_drug_delta_days));


              IF cond_era.condition_era_start_date + this_drug_delta_days
                <= this_person_end_date THEN
                max_drug_era_id := max_drug_era_id + 1;
                INSERT INTO osim_tmp_drug_era
                 (drug_era_id, drug_era_start_date, drug_era_end_date, person_id,
                  drug_concept_id, drug_exposure_count)
                VALUES(
                  max_drug_era_id,
                  cond_era.condition_era_start_date + this_drug_delta_days,
                  cond_era.condition_era_start_date + this_drug_delta_days,
                  cond_era.person_id,
                  this_drug_concept,
                  1);

                this_person_drug_count := this_person_drug_count + 1;
                INSERT INTO this_person_drugs VALUES (this_drug_concept);
                --INSERT INTO this_person_drugs_table VALUES (this_drug_concept);
                IF this_person_drug_count >= this_target_drug_count THEN
                  --IF normal_rand(1, 0, 1) <= 0.8 THEN
                    EXIT cond_loop;
                  --END IF;
                END IF;

              END IF;

            END IF;

          END LOOP;

        END LOOP;
      END;

      --Prevent Deadlock
      IF random() <= 0.2 THEN
        this_target_drug_count := this_target_drug_count - 1;
      END IF;

    END LOOP;

    DECLARE
      drug_eras_cur CURSOR FOR
        SELECT drug.drug_concept_id, drug.drug_era_start_date
        FROM osim_tmp_drug_era drug
        WHERE person_id = max_person_id
        FOR UPDATE NOWAIT; -- OF drug_era_end_date NOWAIT;
      tmp_drug_concept_id       INTEGER;
      tmp_drug_era_start_date   DATE;
      tmp_drug_era_end_date     DATE;
      tmp_era_duration          INTEGER;
      tmp_days_remaining        INTEGER;
      tmp_days_remaining_bucket INTEGER;
      tmp_drug_eras_max         INTEGER;
      tmp_drug_eras_for_drug    INTEGER;
      tmp_age                   INTEGER;
      tmp_age_bucket            INTEGER;
      tmp_total_exposure        INTEGER;
      tmp_drug_duration         INTEGER;
      tmp_gap_duration          INTEGER;
      tmp_gaps                  INTEGER;
      tmp_gap                   INTEGER;
      tmp_duration_start        DATE;
      tmp_duration_end          DATE;
    BEGIN

      FOR drug_eras IN drug_eras_cur LOOP

        tmp_drug_concept_id := drug_eras.drug_concept_id;
        tmp_drug_era_start_date := drug_eras.drug_era_start_date;
        tmp_drug_era_end_date := tmp_drug_era_start_date;

        tmp_days_remaining := this_person_end_date - tmp_drug_era_start_date;
        tmp_days_remaining_bucket :=
          osim__time_observed_bucket(tmp_days_remaining);

        tmp_age := this_age + (tmp_drug_era_start_date
          - this_person_begin_date) / 365.25;
        tmp_age_bucket := osim__age_bucket(tmp_age);

        BEGIN
          tmp_rand := random();
          SELECT DISTINCT
            FIRST_VALUE(drug_era_count)
              OVER (ORDER BY accumulated_probability),
            FIRST_VALUE(total_exposure)
              OVER (ORDER BY accumulated_probability)
          INTO STRICT tmp_drug_eras_max, tmp_total_exposure
          FROM osim_drug_era_count_prob
          WHERE drug_concept_id = tmp_drug_concept_id
            AND drug_count_bucket = this_target_drug_bucket
            AND condition_count_bucket = this_cond_count_bucket
            AND age_range = tmp_age_bucket
            AND time_remaining = tmp_days_remaining_bucket
            AND tmp_rand <= accumulated_probability;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            tmp_drug_eras_max := 1;
            tmp_total_exposure := 0;
        END;

        IF tmp_drug_eras_max = 1 THEN
          tmp_drug_duration := tmp_total_exposure;
        ELSE
          BEGIN
            tmp_rand := random();
            SELECT DISTINCT
              FIRST_VALUE(total_duration)
                OVER (ORDER BY accumulated_probability)
            INTO STRICT tmp_drug_duration
            FROM osim_drug_duration_probability
            WHERE drug_concept_id = tmp_drug_concept_id
              AND time_remaining = tmp_days_remaining_bucket
              AND drug_era_count = tmp_drug_eras_max
              AND total_exposure = tmp_total_exposure
              AND tmp_rand <= accumulated_probability;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              tmp_drug_duration := ((drug_persistence + 1) * (tmp_drug_eras_max - 1)) + tmp_total_exposure;
              tmp_drug_duration := tmp_drug_duration
                + random() * ((this_person_end_date - tmp_drug_era_start_date) - tmp_drug_duration);
          END;
        END IF;

        tmp_total_exposure := osim__randomize_days(tmp_total_exposure);
        tmp_drug_duration := osim__randomize_days(tmp_drug_duration);
        tmp_drug_eras_for_drug := 1;

        tmp_duration_start := tmp_drug_era_start_date;
        tmp_duration_end := tmp_duration_start + tmp_drug_duration;

        IF tmp_total_exposure > tmp_drug_duration - ((drug_persistence + 1) * (tmp_drug_eras_max - 1)) THEN
          tmp_total_exposure := tmp_drug_duration - ((drug_persistence + 1) * (tmp_drug_eras_max - 1));
        END IF;

        WHILE tmp_drug_eras_for_drug <= tmp_drug_eras_max
        LOOP

          tmp_gap_duration := tmp_drug_duration - tmp_total_exposure;
          tmp_gaps := tmp_drug_eras_max - tmp_drug_eras_for_drug;
          IF tmp_gaps = 0 THEN
            tmp_era_duration := tmp_total_exposure;
          ELSE
            tmp_era_duration :=
              ROUND(normal_rand(1, 0, 1) * (tmp_total_exposure / (tmp_gaps + 1))
                + (tmp_total_exposure / (tmp_gaps + 1)));
            IF tmp_era_duration < 0 THEN
              tmp_era_duration := 0;
            END IF;
            IF tmp_era_duration > tmp_total_exposure THEN
              tmp_era_duration := tmp_total_exposure;
            END IF;

            tmp_gap := tmp_gap_duration - ( (drug_persistence + 1) * tmp_gaps);
            tmp_gap :=
              ROUND(normal_rand(1, 0, 1) * (tmp_gap / tmp_gaps)
                + (tmp_gap / tmp_gaps));

            tmp_gap := tmp_gap + (drug_persistence + 1);

            IF tmp_gap > tmp_gap_duration - ( (drug_persistence + 1) * (tmp_gaps-1)) THEN
              tmp_gap := tmp_gap_duration - ( (drug_persistence + 1) * (tmp_gaps-1));
            END IF;

            IF tmp_gap < (drug_persistence + 1) THEN
              tmp_gap := (drug_persistence + 1);
            END IF;

          END IF;

          IF tmp_drug_eras_for_drug = 1 THEN
            -- Update already existing first era
            tmp_drug_era_end_date := tmp_drug_era_start_date + tmp_era_duration;

            UPDATE osim_tmp_drug_era
            SET drug_era_end_date = tmp_drug_era_end_date
            WHERE CURRENT OF drug_eras_cur;

            tmp_duration_start := tmp_drug_era_end_date + tmp_gap;
          ELSE
            IF MOD(tmp_drug_eras_for_drug,2) = 0 THEN
              -- insert at the beginning of the duration
              tmp_drug_era_start_date := tmp_duration_start;
              tmp_drug_era_end_date := tmp_duration_start + tmp_era_duration;
              tmp_duration_start := tmp_drug_era_end_date + tmp_gap;
            ELSE
              -- insert at the end of the duration
              tmp_drug_era_end_date := tmp_duration_end;
              tmp_drug_era_start_date := tmp_drug_era_end_date - tmp_era_duration;
              tmp_duration_end := tmp_drug_era_start_date - tmp_gap;
            END IF;
            --insert drug era
            IF tmp_drug_era_start_date <= this_person_end_date THEN
              IF tmp_drug_era_end_date > this_person_end_date THEN
                tmp_drug_era_end_date := this_person_end_date;
              END IF;
              max_drug_era_id := max_drug_era_id + 1;
              INSERT INTO osim_tmp_drug_era
                (drug_era_id, drug_era_start_date, drug_era_end_date, person_id,
                 drug_concept_id, drug_exposure_count)
              VALUES( max_drug_era_id, tmp_drug_era_start_date, tmp_drug_era_end_date,
                      max_person_id,
--                       db_drug_era_type_code,
                      tmp_drug_concept_id, 1);
            END IF;
          END IF;

          tmp_drug_duration := tmp_duration_end - tmp_duration_start;
          tmp_total_exposure  := tmp_total_exposure - tmp_era_duration;
          tmp_drug_eras_for_drug := tmp_drug_eras_for_drug + 1;

        END LOOP;

      END LOOP;

    END;
--     PERFORM insert_log('Simulated drug',
--         'ins_sim_drug');

  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION ins_sim_data (
  /*=========================================================================
  | PROCEDURE ins_sim_data(person_count)
  |
  | Simualte person_count persons, populating the following tables:
  |   OSIM_PERSON
  |   OSIM_OBSERVATION_PERIOD
  |   OSIM_CONDITION_ERA
  |   OSIM_DRUG_ERA
  |
  |==========================================================================
  */
    person_count                  INTEGER DEFAULT 5000,
    person_start_id               INTEGER DEFAULT 0
  )
  RETURNS VOID AS $$
  DECLARE
    my_sid                        VARCHAR;
    my_start_time                 DATE;
    num_rows                      INTEGER;
    MESSAGE                       TEXT;
    max_persons                   INTEGER;
    db_min_date                   DATE;
    db_max_date                   DATE;
    db_min_year                   INTEGER;
    db_max_year                   INTEGER;

    commit_count_max              INTEGER;
    commit_count                  INTEGER;

    message_count_max             INTEGER;
    message_count                 INTEGER;

    osim_db_persons               INTEGER;

    max_person_id                 INTEGER;
    max_condition_era_id          INTEGER;
    max_drug_era_id               INTEGER;
    max_procedure_occurrence_id   INTEGER;

    person_index                  INTEGER;
    this_gender                   INTEGER;
    this_age                      INTEGER;
    this_age_bucket               INTEGER;
    this_year_of_birth            INTEGER;
    this_person_cond_count        INTEGER;
    this_cond_count_bucket        INTEGER;
    this_obs_days                 INTEGER;
    this_obs_bucket               INTEGER;

    this_person_begin_date        DATE;
    this_person_end_date          DATE;

    drug_persistence              INTEGER;
    cond_persistence              INTEGER;


  BEGIN
    commit_count_max := 10;
    commit_count := 0;

    message_count_max := 1000;
    message_count := 0;

    PERFORM insert_log('Starting to generate simulated data',
        'ins_sim_data');

    PERFORM drop_osim_indexes();

    max_persons := person_count;

    SELECT * from sim_initialization(person_start_id) INTO
    my_sid, my_start_time,
    db_min_date, db_max_date, db_min_year, db_max_year,
    max_person_id, max_condition_era_id, max_drug_era_id, max_procedure_occurrence_id,
    osim_db_persons, drug_persistence, cond_persistence;

    -- Create temp tables to store simulation for each person
    --===================================================================
    --TEMP TABLES
    --===================================================================

    DROP TABLE IF EXISTS osim_tmp_outcome;
    CREATE TABLE osim_tmp_outcome (
          person_id NUMERIC(12, 0) NOT NULL,
          drug_era_id NUMERIC(12, 0) NOT NULL,
          condition_era_id NUMERIC(12, 0) NOT NULL
        ) --ON COMMIT DELETE ROWS
    ;

    DROP TABLE IF EXISTS osim_tmp_condition_era;
    CREATE TABLE osim_tmp_condition_era (
          condition_era_id NUMERIC(15, 0) NOT NULL,
          condition_era_start_date DATE,
          person_id NUMERIC(12, 0) NOT NULL,
          confidence NUMERIC,
          condition_era_end_date DATE,
          condition_concept_id NUMERIC(15, 0),
          condition_occurrence_count NUMERIC(5, 0)
        ) --ON COMMIT DELETE ROWS
    ;

    CREATE INDEX osim_cond ON osim_tmp_condition_era (person_id)
      WITH (FILLFACTOR = 90);
--     CREATE INDEX osim_cond1 ON osim_tmp_condition_era (person_id, condition_era_start_date)
--       WITH (FILLFACTOR = 90);

    DROP TABLE IF EXISTS osim_tmp_drug_era;
    CREATE TABLE osim_tmp_drug_era (
          drug_era_id NUMERIC(15, 0) NOT NULL,
          drug_era_start_date DATE,
          drug_era_end_date DATE,
          person_id NUMERIC(12, 0) NOT NULL,
          drug_concept_id NUMERIC(15, 0),
          drug_exposure_count NUMERIC(5, 0)
        ) --ON COMMIT DELETE ROWS
    ;
    CREATE INDEX osim_drug ON osim_tmp_drug_era (person_id)
      WITH (FILLFACTOR = 90);

    DROP TABLE IF EXISTS osim_tmp_procedure_occurrence;
    CREATE TABLE osim_tmp_procedure_occurrence (
          procedure_occurrence_id NUMERIC(15, 0) NOT NULL,
          procedure_date DATE,
          person_id NUMERIC(12, 0) NOT NULL,
          procedure_concept_id NUMERIC(15, 0),
          quantity NUMERIC(5, 0)
        ) --ON COMMIT DELETE ROWS
    ;
    CREATE INDEX osim_proc ON osim_tmp_procedure_occurrence (person_id)
      WITH (FILLFACTOR = 90);

    -- Start the simulation
    person_index := 1;
    WHILE person_index <= max_persons
    LOOP


      --
      -- Create osim person
      --
      SELECT * FROM ins_sim_person(db_min_year, max_person_id) INTO max_person_id,
        this_gender, this_age, this_age_bucket, this_year_of_birth;

      --
      -- Create Observation Period
      --
      SELECT * FROM ins_sim_observation_period(max_person_id, this_gender, this_age,
        db_min_date, db_max_date, this_year_of_birth) INTO
        this_year_of_birth, this_person_cond_count, this_cond_count_bucket,
        this_obs_bucket, this_obs_days, this_person_begin_date, this_person_end_date;

      --
      -- Simulate Conditions
      --

      SELECT * FROM ins_sim_conditions (max_person_id, this_person_begin_date,
      this_age, this_gender, this_obs_days, this_person_cond_count, max_condition_era_id,this_cond_count_bucket)
      INTO this_person_cond_count, max_condition_era_id, this_cond_count_bucket;

      --
      -- Simulate Drugs: Simulated from the person's conditions
      --
      SELECT * FROM ins_sim_drugs(max_person_id, this_person_begin_date, max_drug_era_id,
        this_person_end_date, this_gender, this_cond_count_bucket, this_age,
        this_age_bucket, drug_persistence) INTO max_drug_era_id;

      --
      -- Simulate Procedures: Simulated from the person's conditions
      --
      SELECT * FROM ins_sim_procedures(max_person_id, this_person_begin_date, max_procedure_occurrence_id,
        this_person_end_date, this_gender, this_cond_count_bucket, this_age,
        this_age_bucket) INTO max_procedure_occurrence_id;

      --
      -- Copy Eras from Temp Tables
      --
      -- insert conditions
      INSERT INTO osim_condition_era
       (condition_era_id, condition_era_start_date, condition_era_end_date,
        person_id, condition_concept_id,
        condition_occurrence_count)
      SELECT
        condition_era_id, condition_era_start_date, condition_era_end_date,
        person_id, condition_concept_id,
        condition_occurrence_count
      FROM osim_tmp_condition_era
      WHERE person_id = max_person_id;

      -- insert drugs
      INSERT INTO osim_drug_era
       (drug_era_id, drug_era_start_date, drug_era_end_date, person_id,
       drug_concept_id, drug_exposure_count)
      SELECT
        -- max_drug_era_id + coalesce(row_number() OVER (ORDER BY person_id), 0) AS
        drug_era_id,
        drug_era_start_date, drug_era_end_date, person_id,
        drug_concept_id, drug_exposure_count
      FROM osim_tmp_drug_era
      WHERE person_id = max_person_id;

--       GET DIAGNOSTICS num_rows = ROW_COUNT;
--       max_drug_era_id := max_drug_era_id + num_rows;

      -- insert procedures
      INSERT INTO osim_procedure_occurrence
       (procedure_occurrence_id, procedure_date, person_id,
       procedure_concept_id, quantity)
      SELECT
        --max_procedure_occurrence_id + coalesce(row_number() OVER (ORDER BY person_id), 0) AS
        procedure_occurrence_id,
        procedure_date, person_id,
        procedure_concept_id, quantity
      FROM osim_tmp_procedure_occurrence
      WHERE person_id = max_person_id;

--       GET DIAGNOSTICS num_rows = ROW_COUNT;
--       max_procedure_occurrence_id := max_procedure_occurrence_id + num_rows;

      --Report progress every 1%
      IF FLOOR(person_index/(0.01*person_count)) < FLOOR((person_index+1)/(0.01*person_count)) THEN
        raise notice ' persons completed(%).', FLOOR((person_index+1)/(0.01*person_count));
        PERFORM insert_log('SID=' || my_sid || ': ' || to_char(person_index+1, '999999') || ' persons completed(' ||
          FLOOR((person_index+1)/(0.01*person_count)) || '%).',
          'ins_sim_data');
      END IF;
      person_index := person_index + 1;

--       TRUNCATE TABLE osim_tmp_procedure_occurrence;
--       TRUNCATE TABLE osim_tmp_condition_era;
--       TRUNCATE TABLE osim_tmp_drug_era;

    END LOOP;


    person_index := person_index - 1;
    PERFORM insert_log('Processing complete.  ' || person_index
                     || ' persons created.', 'ins_sim_data');
  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_sim_data');
    DISCARD TEMP; --delete all temp tables if commit doesn't happen
    GET STACKED DIAGNOSTICS MESSAGE = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'context: >>%<<', MESSAGE;
    raise notice '% %', SQLERRM, SQLSTATE;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ins_outcomes()
  /*=========================================================================
  | PROCEDURE ins_outcomes
  |
  | Add outcomes defined in the OSIM_DRUG_OUTCOME table.
  |
  |  OSIM_DRUG_OUTCOME table is manually updated with the following columns:
  | 	 drug_concept_id ? outcome drug concept id

  |    condition_concept_id - outcome condition concept id
  |
  | 	 relative_risk ? % of persons on outcome drug with outcome condition
  |       during time at risk
  |
  |    outcome_risk_type
  |      first exposure ? outcomes are only added to first drug exposures
  |      any exposure ? outcomes may be added to any drug exposure
  |      insidious ? outcomes are randomly added on a date during any exposure
  |      accumulative ? outcomes are added during any drug exposure, with
  |           accumulating probability over time
  |
  |    outcome_onset_days_min ? minimum days from drug exposure start date for outcome to occur
  |      this column can be set for any outcome_risk_type
  |
  |    outcome_onset_days_max ? maximum days from drug exposure start date for outcome to occur
  |      this column only applies to first and any exposure types
  |      value must be >= outcome_onset_days_min
  |
  |==========================================================================
  */
  RETURNS VOID AS $$
  DECLARE
    outcome_cur CURSOR FOR
      SELECT risk_or_benefit, drug_concept_id, condition_concept_id, relative_risk,
        outcome_risk_type, outcome_onset_days_min, outcome_onset_days_max
      FROM osim_drug_outcome;
    tmp_persons_with_drug      INTEGER;
    tmp_drug_eras              INTEGER;
    tmp_persons_with_outcome   INTEGER;
    tmp_current_prevalence     INTEGER;
    tmp_target_rows            INTEGER;
    max_condition_era_id       INTEGER;
    outcomes_insert_count      INTEGER;
    --outcomes_to_remove         TAB_OUTCOME;
    db_cond_era_type_code      VARCHAR(3);
    MESSAGE                    text;
    num_rows          INTEGER;
  BEGIN
    PERFORM insert_log('Starting Outcome Analysis/Creation', 'ins_outcomes');

    SELECT MAX(condition_era_id)
    INTO max_condition_era_id
    FROM osim_condition_era;

--     SELECT condition_occurrence_type
--     INTO db_cond_era_type_code
--     FROM osim_src_db_attributes
--     WHERE ROWNUM = 1;

    FOR outcome_row IN outcome_cur LOOP

      outcomes_insert_count := 0;

      SELECT COUNT(DISTINCT drug.person_id)
      INTO tmp_persons_with_drug
      FROM osim_drug_era drug
      WHERE drug.drug_concept_id = outcome_row.drug_concept_id;

      SELECT COUNT(DISTINCT drug_era_id)
      INTO tmp_drug_eras
      FROM osim_drug_era drug
      WHERE drug.drug_concept_id = outcome_row.drug_concept_id;

      SELECT COUNT(DISTINCT person_id)
      INTO tmp_persons_with_outcome
      FROM osim__get_outcome_drug_eras(outcome_row.drug_concept_id,
                                              outcome_row.condition_concept_id,
                                              outcome_row.outcome_risk_type,
                                              outcome_row.outcome_onset_days_min,
                                              outcome_row.outcome_onset_days_max);

      IF tmp_persons_with_drug > 0 THEN
        tmp_current_prevalence := tmp_persons_with_outcome / tmp_persons_with_drug;
      ELSE
        tmp_current_prevalence := 0.0;
      END IF;

      MESSAGE := 'Processing ' || outcome_row.outcome_risk_type ||
        ' ' || outcome_row.risk_or_benefit ||
        ' outcomes for drug_concept_id=' || outcome_row.drug_concept_id ||
        ', condition_concept_id=' || outcome_row.condition_concept_id || '.' ||
        'outcome_onset_days_min=' || outcome_row.outcome_onset_days_min || '. ' ||
        'outcome_onset_days_max=' || outcome_row.outcome_onset_days_max || '.';
      PERFORM insert_log(MESSAGE, 'ins_outcomes');

      MESSAGE := 'Currently there are: ' || tmp_drug_eras || ' existing drug eras, ' ||
        tmp_persons_with_drug || ' persons with drug, ' ||
        tmp_persons_with_outcome || ' persons already with outcome. ' ||
        'Current Prevalence=' || TO_CHAR(tmp_current_prevalence, '9.99999') || '. ' ||
        'Relative Risk=' || TO_CHAR(outcome_row.relative_risk, '9.99999') || '.';

      PERFORM insert_log(MESSAGE, 'ins_outcomes');

      -- Randomized INTEGER of outcomes to attempt to add (or remove)
      tmp_target_rows :=
        ROUND(((DBMS_RANDOM.NORMAL
        * 0.05 * tmp_persons_with_drug * outcome_row.relative_risk)/1.645)
        + (tmp_persons_with_drug
            * (outcome_row.relative_risk - tmp_current_prevalence)));

      IF tmp_target_rows < 0 AND outcome_row.risk_or_benefit = 'benefit' THEN
      --prevenatative
        tmp_target_rows := -tmp_target_rows;

        INSERT INTO osim_tmp_outcome
          SELECT person_id, drug_era_id, condition_era_id
          FROM
           (SELECT /*+ materialize */ *
            FROM osim__get_outcome_eras(outcome_row.drug_concept_id,
                                              outcome_row.condition_concept_id,
                                              outcome_row.outcome_risk_type,
                                              outcome_row.outcome_onset_days_min,
                                              outcome_row.outcome_onset_days_max)

            ORDER BY random()) t1
          LIMIT tmp_target_rows;

        DELETE FROM osim_condition_era cond
        WHERE EXISTS
          (SELECT 1
           FROM osim_tmp_outcome tmp_outc
           WHERE cond.condition_era_id = tmp_outc.condition_era_id
           AND cond.person_id = tmp_outc.person_id);

        outcomes_insert_count := -num_rows;

        MESSAGE := outcomes_insert_count || ' ' || outcome_row.outcome_risk_type ||
          ' outcomes removed for drug_concept_id=' || outcome_row.drug_concept_id ||
          ', condition_concept_id=' || outcome_row.condition_concept_id || '.';
        PERFORM insert_log(MESSAGE, 'ins_outcomes');

      END IF;

      IF tmp_target_rows >= 0 AND outcome_row.risk_or_benefit = 'risk' THEN

        CASE outcome_row.outcome_risk_type
          WHEN 'first exposure' THEN
          -- add outcome to the risk period for the first drug era per selected person
            INSERT INTO osim_condition_era
             (condition_era_id, condition_era_start_date, condition_era_end_date,
              person_id, condition_concept_id,
              condition_occurrence_count)
            WITH outc AS
             (SELECT /*+ materialize */ *
              FROM osim__get_outcome_drug_eras(outcome_row.drug_concept_id,
                                                outcome_row.condition_concept_id,
                                                outcome_row.outcome_risk_type,
                                                outcome_row.outcome_onset_days_min,
                                                outcome_row.outcome_onset_days_max))
            SELECT
              max_condition_era_id + ROWNUM AS condition_era_id,
              condition_era_start_date,
              condition_era_start_date AS condition_era_end_date,
              person_id,
              outcome_row.condition_concept_id AS condition_concept_id,
--               db_cond_era_type_code,
              1
            FROM
             (SELECT
                person_id,
                drug_era_start_date
                  + ROUND((outcome_row.outcome_onset_days_max
                      - outcome_row.outcome_onset_days_min)
                  * normal_rand(1, 0, 1)) AS condition_era_start_date,
                observation_period_end_date
              FROM
               (SELECT
                  person_id,
                  drug_era_start_date,
                  observation_period_end_date
                FROM
                 (SELECT
                    drug.person_id,
                    MIN(drug.drug_era_start_date) AS drug_era_start_date,
                    obs.observation_period_end_date
                  FROM osim_drug_era drug
                  INNER JOIN osim_observation_period obs
                    ON drug.person_id = obs.person_id
                  LEFT JOIN outc
                    ON drug.drug_era_id = outc.drug_era_id
                  WHERE drug.drug_concept_id = outcome_row.drug_concept_id
                    AND outc.drug_era_id IS NULL
                  GROUP BY drug.person_id, obs.observation_period_end_date
                  ORDER BY random()) t1
                  LIMIT tmp_target_rows) t2
             ) t3
              WHERE condition_era_start_date <= observation_period_end_date;

            outcomes_insert_count := num_rows;

          WHEN 'any exposure' THEN -- add an outcome to risk period per selected person
            INSERT INTO osim_condition_era
             (condition_era_id, condition_era_start_date, condition_era_end_date,
              person_id, condition_concept_id,
              condition_occurrence_count)
            WITH
              outc AS
               (SELECT /*+ materialize */ DISTINCT person_id
                FROM osim__get_outcome_drug_eras(outcome_row.drug_concept_id,
                                                  outcome_row.condition_concept_id,
                                                  outcome_row.outcome_risk_type,
                                                  outcome_row.outcome_onset_days_min,
                                                  outcome_row.outcome_onset_days_max)),
              outcome_draw AS
               (SELECT
                  person_id,
                  drug_concept_id,
                  eras,
                  CEIL(random() * eras) AS outcome_exposure
                FROM
                 (SELECT
                    drug.person_id,
                    drug.drug_concept_id,
                    COUNT(drug.drug_era_id) AS eras
                  FROM osim_drug_era drug
                  LEFT JOIN outc ON drug.person_id = outc.person_id
                  WHERE drug_concept_id = outcome_row.drug_concept_id
                    AND outc.person_id IS NULL
                  GROUP BY drug.person_id, drug.drug_concept_id
                ORDER BY random(), 1) t3
                LIMIT tmp_target_rows)
              SELECT
                max_condition_era_id + ROWNUM AS condition_era_id,
                condition_era_start_date,
                condition_era_start_date AS condition_era_end_date,
                person_id,
                outcome_row.condition_concept_id AS condition_concept_id,
--                 db_cond_era_type_code,
                1
              FROM
              (SELECT
                  person_id,
                  risk_start_date + CEIL(normal_rand(1, 0, 1)
                    * (risk_end_date - risk_start_date)) AS condition_era_start_date
                FROM
                 (SELECT
                    drug.person_id,
                    drug_era_start_date + coalesce(outcome_row.outcome_onset_days_min,0)
                      AS risk_start_date,
                    drug_era_start_date + coalesce(outcome_row.outcome_onset_days_max,0)
                      AS risk_end_date,
                    outcome_draw.outcome_exposure,
                    SUM(1)
                      OVER (PARTITION BY drug.person_id, drug.drug_concept_id
                            ORDER BY drug_era_start_date ROWS UNBOUNDED PRECEDING)
                              AS era_seq
                  FROM outcome_draw
                  INNER JOIN osim_drug_era drug
                    ON outcome_draw.person_id = drug.person_id
                      AND outcome_draw.drug_concept_id = drug.drug_concept_id) t1
                WHERE era_seq = outcome_exposure) t2;

            outcomes_insert_count := num_rows;

          WHEN 'accumulative' THEN -- favor later drug exposures for outcome
            -- Use trianglar matrix to assign probability
            -- Each day at risk will have its own ordinal value INTEGER of chances
            INSERT INTO osim_condition_era
             (condition_era_id, condition_era_start_date, condition_era_end_date,
              person_id, condition_concept_id,
              condition_occurrence_count)
            WITH
              outc AS
               (SELECT /*+ materialize */ DISTINCT person_id
                FROM osim__get_outcome_drug_eras(outcome_row.drug_concept_id,
                                                  outcome_row.condition_concept_id,
                                                  outcome_row.outcome_risk_type,
                                                  outcome_row.outcome_onset_days_min,
                                                  outcome_row.outcome_onset_days_max)),
              outcome_draw AS
               (SELECT
                  person_id,
                  drug_concept_id,
                  eras,
                  exposure_days,
                  (exposure_days * exposure_days + exposure_days) / 2 AS accumulated_risk,
                  -- Quadratic Formula to find exposure day of Outcome
                  -- Each successive day has its ordinal of chances
                  -- The accumulated chances on any day are (n**2 + n) / 2
                  -- To determine the ordinal day of the randomly chosen chance (r):
                  --   a = 1/2, -b = - 1/2, c = -r
                  -- Should this use a gentler accumulation?
                  CEIL(SQRT(0.25+2*CEIL(normal_rand(1, 0, 1)
                    * (exposure_days * exposure_days + exposure_days) / 2))-0.5)
                    + coalesce(outcome_row.outcome_onset_days_min,0) AS outcome_day
                FROM
                 (SELECT
                    drug.person_id,
                    drug.drug_concept_id,
                    COUNT(drug_era_id) AS eras,
                    SUM(ROUND(1 + drug.drug_era_end_date - drug.drug_era_start_date))
                      - coalesce(outcome_row.outcome_onset_days_min,0) AS exposure_days
                  FROM osim_drug_era drug
                  LEFT JOIN outc ON drug.person_id = outc.person_id
                  WHERE drug_concept_id = outcome_row.drug_concept_id
                    AND outc.person_id IS NULL
                  GROUP BY drug.person_id, drug.drug_concept_id
                ORDER BY normal_rand(1, 0, 1), 1) t1
                WHERE exposure_days > 0
                LIMIT tmp_target_rows),
              era_accum AS
               (SELECT
                  drug.person_id,
                  drug.drug_era_id,
                  drug.drug_concept_id,
                  SUM(ROUND(1 + drug.drug_era_end_date - drug.drug_era_start_date))
                    OVER
                     (PARTITION BY person_id, drug_concept_id
                      ORDER BY drug_era_start_date ASC
                        ROWS UNBOUNDED PRECEDING) accum_days
                FROM osim_drug_era drug
                WHERE drug_concept_id = outcome_row.drug_concept_id
                ORDER BY 1)
              SELECT
                max_condition_era_id + ROWNUM AS condition_era_id,
                condition_era_start_date,
                condition_era_start_date AS condition_era_end_date,
                person_id,
                outcome_row.condition_concept_id AS condition_concept_id,
--                 db_cond_era_type_code,
                1
              FROM
               (SELECT DISTINCT
                  outcome_draw.person_id,
                  FIRST_VALUE(drug_era_end_date)
                    OVER (PARTITION BY era_accum.person_id, era_accum.drug_concept_id
                          ORDER BY accum_days) -
                  (FIRST_VALUE(accum_days)
                    OVER (PARTITION BY era_accum.person_id, era_accum.drug_concept_id
                          ORDER BY accum_days) - outcome_draw.outcome_day)
                            AS condition_era_start_date
                FROM outcome_draw
                INNER JOIN era_accum
                  ON outcome_draw.person_id = era_accum.person_id
                  AND outcome_draw.drug_concept_id = era_accum.drug_concept_id
                INNER JOIN osim_drug_era drug
                  ON era_accum.drug_era_id = drug.drug_era_id
                WHERE outcome_day <= accum_days) t4;

            outcomes_insert_count := num_rows;

          WHEN 'insidious' THEN -- random during any exposure
            INSERT INTO osim_condition_era
             (condition_era_id, condition_era_start_date, condition_era_end_date,
              person_id, condition_concept_id,
              condition_occurrence_count)
            WITH
              outc AS
               (SELECT /*+ materialize */ DISTINCT person_id
                FROM osim__get_outcome_drug_eras(outcome_row.drug_concept_id,
                                                  outcome_row.condition_concept_id,
                                                  outcome_row.outcome_risk_type,
                                                  outcome_row.outcome_onset_days_min,
                                                  outcome_row.outcome_onset_days_max)),
              outcome_draw AS
               (SELECT
                  person_id,
                  drug_concept_id,
                  eras,
                  exposure_days,
                  CEIL(normal_rand(1, 0, 1) * exposure_days)
                    + coalesce(outcome_row.outcome_onset_days_min,0) AS outcome_day
                FROM
                 (SELECT
                    drug.person_id,
                    drug.drug_concept_id,
                    COUNT(drug_era_id) AS eras,
                    SUM(ROUND(1 + drug.drug_era_end_date - drug.drug_era_start_date))
                      - coalesce(outcome_row.outcome_onset_days_min,0) AS exposure_days
                  FROM osim_drug_era drug
                  LEFT JOIN outc ON drug.person_id = outc.person_id
                  WHERE drug_concept_id = outcome_row.drug_concept_id
                    AND outc.person_id IS NULL
                  GROUP BY drug.person_id, drug.drug_concept_id
                ORDER BY normal_rand(1, 0, 1), 1) t1
                WHERE exposure_days > 0
                LIMIT tmp_target_rows),
              era_accum AS
               (SELECT
                  drug.person_id,
                  drug.drug_era_id,
                  drug.drug_concept_id,
                  SUM(ROUND(1 + drug.drug_era_end_date - drug.drug_era_start_date))
                    OVER
                     (PARTITION BY person_id, drug_concept_id
                      ORDER BY drug_era_start_date ASC
                        ROWS UNBOUNDED PRECEDING) accum_days
                FROM osim_drug_era drug
                WHERE drug_concept_id = outcome_row.drug_concept_id
                ORDER BY 1)
              SELECT
                max_condition_era_id + ROWNUM AS condition_era_id,
                condition_era_start_date,
                condition_era_start_date AS condition_era_end_date,
                person_id,
                outcome_row.condition_concept_id AS condition_concept_id,
--                 db_cond_era_type_code,
                1
              FROM
               (SELECT DISTINCT
                  outcome_draw.person_id,
                  FIRST_VALUE(drug_era_end_date)
                    OVER (PARTITION BY era_accum.person_id, era_accum.drug_concept_id
                          ORDER BY accum_days) -
                  (FIRST_VALUE(accum_days)
                    OVER (PARTITION BY era_accum.person_id, era_accum.drug_concept_id
                          ORDER BY accum_days) - outcome_draw.outcome_day)
                            AS condition_era_start_date
                FROM outcome_draw
                INNER JOIN era_accum
                  ON outcome_draw.person_id = era_accum.person_id
                  AND outcome_draw.drug_concept_id = era_accum.drug_concept_id
                INNER JOIN osim_drug_era drug
                  ON era_accum.drug_era_id = drug.drug_era_id
                WHERE outcome_day <= accum_days) t2;

            outcomes_insert_count := num_rows;

        END CASE;

        MESSAGE := outcomes_insert_count || ' ' || outcome_row.outcome_risk_type ||
          ' outcomes created for drug_concept_id=' || outcome_row.drug_concept_id ||
          ', condition_concept_id=' || outcome_row.condition_concept_id || '.';
        PERFORM insert_log(MESSAGE, 'ins_outcomes');

      END IF;
      --COMMIT;
    END LOOP;
    
    PERFORM insert_log('Processing complete', 'ins_outcomes');
  EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_outcomes');
  END; 
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ins_sim_procedures (
  /*=========================================================================
  | PROCEDURE ins_sim_procedures
  |
  |  Simulate Procedure Occurrence
  |==========================================================================
  */
    max_person_id                 INTEGER,
    this_person_begin_date        DATE,
    INOUT max_procedure_occurrence_id            INTEGER,
    this_person_end_date          DATE,
    this_gender                   INTEGER,
    this_cond_count_bucket        INTEGER,
    this_age                      INTEGER,
    this_age_bucket               INTEGER
  )
  RETURNS INTEGER AS $$
  DECLARE

    tmp_rand                      FLOAT;
    this_person_procedures        INTEGER ARRAY;
    this_occurrence_date   DATE;
    this_occurrence_days_remaining        INTEGER;
    this_occurrence_days_remaining_bucket INTEGER;
    this_procedure_occurrences_max         INTEGER;
    procedure_occurrences_for_procedure    INTEGER;
    this_occurrence_age                   INTEGER;
    this_occurrence_age_bucket            INTEGER;
    this_occurrence_start        DATE;
    this_occurrence_delta_days    INTEGER;


    this_procedure_age_bucket INTEGER;
    this_procedure_age                  INTEGER;
    this_person_procedure_count        INTEGER;
    this_target_procedure_count        INTEGER;
    this_cond_procedure_count_max      INTEGER;
    this_procedure_delta_days          INTEGER;
    this_procedure_days_remaining      INTEGER;
    this_procedure_days_remaining_bucket INTEGER;
    this_target_procedure_bucket       INTEGER;
    this_cond_procedure_count          INTEGER;
    this_procedure_concept             INTEGER;

  BEGIN
    -- Add else part if all else fails
    --
    -- Simulate procedures
    --   procedures are simulated from the person's Conditons

    -- Clear existing condition and procedure concepts for new person
--     TRUNCATE TABLE this_person_procedures_table;
    this_person_procedures := '{}';

    -- Draw for person's Distinct procedure Concept count
    BEGIN
      tmp_rand := random();
      SELECT DISTINCT FIRST_VALUE(procedure_count)
        OVER (ORDER BY accumulated_probability)
      INTO STRICT this_target_procedure_count
      FROM osim_procedure_count_prob
      WHERE gender_concept_id = this_gender
        AND age_bucket = this_age_bucket
        AND condition_count_bucket = this_cond_count_bucket
        AND tmp_rand <= accumulated_probability;
    EXCEPTION
      WHEN NO_DATA_FOUND
        THEN
          this_target_procedure_count := 0;
    END;


    this_target_procedure_bucket := osim__procedure_count_bucket(this_target_procedure_count);

    this_person_procedure_count := 0;

    --
    -- Begin procedure simulation Loop
    --
    -- raise notice 'Starting main loop';
    -- raise notice 'Max total procedures: %', this_target_procedure_count;

    WHILE this_person_procedure_count < this_target_procedure_count
    LOOP
      DECLARE
        -- Cursor of all condition eras and start needed for transitions
        cond_era_cur CURSOR FOR
          SELECT
            person_id,
            condition_concept_id,
            condition_era_start_date,
            interval_bucket,
            day_cond_count
          FROM
           (SELECT DISTINCT
              cond_lag.person_id,
              coalesce(cond.condition_concept_id,-1) AS condition_concept_id,
              cond_lag.lag_start_date AS condition_era_start_date,
              osim__duration_days_bucket(cond_lag.lag_end_date
                  - cond_lag.lag_start_date) AS interval_bucket,
              cond_lag.day_cond_count
            FROM
             (SELECT
                cond_dates.person_id,
                cond_dates.condition_era_start_date AS lag_start_date,
                LEAD (cond_dates.condition_era_start_date,1,
                  cond_dates.condition_era_start_date+30)
                  OVER (PARTITION BY cond_dates.person_id
                        ORDER BY cond_dates.condition_era_start_date)
                          AS lag_end_date,
                cond_dates.day_cond_count
              FROM
               (SELECT
                  cond.person_id,
                  cond.condition_era_start_date,
                  COUNT (DISTINCT cond.condition_concept_id) AS day_cond_count
                FROM osim_tmp_condition_era cond
                WHERE cond.person_id=max_person_id
                GROUP BY cond.person_id, cond.condition_era_start_date
                UNION
                SELECT person_id, condition_era_start_date, 0
                FROM osim_tmp_condition_era cond
                  WHERE cond.person_id = max_person_id
                  AND cond.condition_era_start_date = this_person_begin_date
                AND cond.condition_era_id IS NULL) cond_dates) cond_lag
            LEFT JOIN osim_tmp_condition_era cond
              ON cond_lag.person_id = cond.person_id
                AND cond_lag.lag_start_date=cond.condition_era_start_date) t1
          ORDER BY 5 DESC;

      BEGIN
        <<cond_loop>>
        FOR cond_era IN cond_era_cur LOOP
          -- Draw for INTEGER of procedure Draws
          BEGIN
--             raise notice 'In condition loop';
            this_procedure_age := (cond_era.condition_era_start_date
              - this_person_begin_date) / 365.25 + this_age;
            this_procedure_age_bucket := osim__age_bucket(this_procedure_age);
            tmp_rand := random();

            SELECT DISTINCT
              FIRST_VALUE(procedure_count)
                OVER (ORDER BY count_prob.accumulated_probability) AS procedure_occurrence_count
            INTO STRICT this_cond_procedure_count_max
            FROM osim_cond_procedure_count_prob count_prob
            WHERE count_prob.condition_concept_id = cond_era.condition_concept_id
              AND count_prob.procedure_count_bucket = this_target_procedure_bucket
              AND count_prob.condition_count_bucket = this_cond_count_bucket
              AND count_prob.interval_bucket = cond_era.interval_bucket
              AND count_prob.age_bucket = this_procedure_age_bucket
              AND tmp_rand <= count_prob.accumulated_probability;
          EXCEPTION
            WHEN NO_DATA_FOUND
              THEN
                this_cond_procedure_count_max := 0;
          END;

          -- Force the last few procedures, if necessary
          -- skipping this section since its not necessary for every condition to have a procedure

--           IF this_target_procedure_count - this_person_procedure_count < 10
--              AND this_cond_procedure_count_max = 0 THEN
--             this_cond_procedure_count_max := 1;
--           END IF;

          this_cond_procedure_count = 1;
          -- raise notice 'This procedure condition max: %', this_cond_procedure_count_max;
          WHILE this_cond_procedure_count < this_cond_procedure_count_max
          LOOP
          --  raise notice '% %', this_cond_procedure_count, this_cond_procedure_count_max;
          -- Draw for procedure Concept
            BEGIN
              tmp_rand := random();
              SELECT DISTINCT
                FIRST_VALUE(procedure_concept_id)
                  OVER (ORDER BY accumulated_probability),
                coalesce(FIRST_VALUE(delta_days)
                  OVER (ORDER BY accumulated_probability),0)
              INTO STRICT this_procedure_concept, this_procedure_delta_days
              FROM osim_cond_first_procedure_prob prob
              WHERE prob.condition_concept_id = cond_era.condition_concept_id
                AND prob.procedure_count_bucket = this_target_procedure_bucket
                AND prob.condition_count_bucket = this_cond_count_bucket
                AND prob.gender_concept_id = this_gender
                AND prob.age_bucket = this_age_bucket
                AND prob.day_cond_count = 2
                  --CASE WHEN cond_era.day_cond_count > 1 THEN 2 ELSE 1 END
                AND tmp_rand <= prob.accumulated_probability;
            EXCEPTION
              WHEN NO_DATA_FOUND
                THEN
                  this_procedure_concept := 0;
                  this_procedure_delta_days := 0;
            END;

            IF this_procedure_concept > 0
               AND NOT EXISTS (SELECT this_procedure_concept = ANY(this_person_procedures)) THEN
               --AND NOT EXISTS (select 1 from this_person_procedures_table where procedure_concept_id = this_procedure_concept) THEN

            --  raise notice 'in procedure inner loop';
              this_procedure_delta_days := (osim__randomize_days(this_procedure_delta_days));
              this_procedure_days_remaining := this_person_end_date - this_occurrence_date;
              this_procedure_days_remaining_bucket = osim__time_observed_bucket(this_occurrence_days_remaining);

              IF cond_era.condition_era_start_date + this_procedure_delta_days
                <= this_person_end_date THEN
                max_procedure_occurrence_id := max_procedure_occurrence_id + 1;
                INSERT INTO osim_tmp_procedure_occurrence
                 (procedure_occurrence_id, procedure_date, person_id,
                  procedure_concept_id, quantity)
                VALUES(
                  max_procedure_occurrence_id,
                  cond_era.condition_era_start_date + this_procedure_delta_days,
                  cond_era.person_id,
                  this_procedure_concept,
                  1);
              --  raise notice 'Inserted first occurrence';
                this_person_procedure_count := this_person_procedure_count + 1;
                this_cond_procedure_count = this_cond_procedure_count + 1;

                --INSERT INTO this_person_procedures_table VALUES (this_procedure_concept);
                INSERT INTO this_person_procedures VALUES (this_procedure_concept);
                -- Insert procedure reoccurrences

                -- initialization

                --this_procedure_concept;
                this_occurrence_date := cond_era.condition_era_start_date + this_procedure_delta_days;
                this_occurrence_days_remaining := this_person_end_date - this_occurrence_date;
                this_occurrence_days_remaining_bucket :=
                    osim__time_observed_bucket(this_occurrence_days_remaining);

                this_occurrence_age := this_age + (this_occurrence_date - this_person_begin_date) / 365.25;
                this_occurrence_age_bucket := osim__age_bucket(this_occurrence_age);

                BEGIN
                    tmp_rand := random();
                    SELECT DISTINCT
                      FIRST_VALUE(procedure_occurrence_count)
                        OVER (ORDER BY accumulated_probability)
                    INTO STRICT this_procedure_occurrences_max
                    FROM osim_procedure_occurrence_count_prob
                    WHERE procedure_concept_id = this_procedure_concept
                      AND procedure_count_bucket = this_target_procedure_bucket
                      AND condition_count_bucket = this_cond_count_bucket
                      AND age_range = this_occurrence_age_bucket
                      AND time_remaining = this_occurrence_days_remaining_bucket
                      AND tmp_rand <= accumulated_probability;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                      this_procedure_occurrences_max := 1;

                END;

                procedure_occurrences_for_procedure := 1;
                this_occurrence_start := this_occurrence_date;

                WHILE procedure_occurrences_for_procedure < this_procedure_occurrences_max
                  LOOP
                    tmp_rand := random();

                   -- Draw for days until the subsequent Procedure Occurrence
                    BEGIN
                      SELECT DISTINCT
                        FIRST_VALUE(delta_days)
                          OVER (ORDER BY accumulated_probability) as delta_days
                      INTO STRICT this_occurrence_delta_days
                      FROM osim_procedure_reoccur_probability prob
                        WHERE procedure_concept_id = this_procedure_concept
                        AND age_range = this_occurrence_age_bucket
                        AND time_remaining = this_occurrence_days_remaining
                        AND tmp_rand <= accumulated_probability;
                    EXCEPTION
                      WHEN NO_DATA_FOUND
                        THEN
                          -- Reset to first occurrence
                          this_occurrence_age_bucket := 0;
                          this_occurrence_date := cond_era.condition_era_start_date + this_procedure_delta_days;
                          this_occurrence_age := (cond_era.condition_era_start_date - this_person_begin_date) / 365.25 + this_age;
                          this_occurrence_days_remaining_bucket := this_procedure_days_remaining_bucket - this_procedure_delta_days;
                          this_procedure_occurrences_max := this_procedure_occurrences_max - 1;
                    END;

                  IF this_occurrence_age_bucket > 0 THEN

                      --
                      -- Write Procedure Occurrence
                      --

                      -- Randomize from returned days bucket
                      this_occurrence_delta_days := (osim__randomize_days(this_occurrence_delta_days));

                      this_occurrence_start := this_occurrence_start + this_occurrence_delta_days;

                      this_occurrence_age := this_occurrence_age + this_occurrence_delta_days / 365.25;

                      this_occurrence_days_remaining_bucket := this_occurrence_days_remaining_bucket - this_occurrence_delta_days;

                    max_procedure_occurrence_id := max_procedure_occurrence_id + 1;
                     INSERT INTO osim_tmp_procedure_occurrence
                       (procedure_occurrence_id, procedure_date, person_id,
                            procedure_concept_id, quantity)
                        SELECT
                          max_procedure_occurrence_id,
                          this_occurrence_start,
                          max_person_id,
                          this_procedure_concept,
                          1
                        ;

                      procedure_occurrences_for_procedure := procedure_occurrences_for_procedure + 1;
                      this_cond_procedure_count := this_cond_procedure_count + 1;

                    END IF;

                    IF this_occurrence_days_remaining_bucket < 0 THEN
                      this_occurrence_date := cond_era.condition_era_start_date;
                      this_occurrence_age := this_procedure_age;
                      this_occurrence_days_remaining_bucket := this_procedure_days_remaining_bucket;
                      this_procedure_occurrences_max := this_procedure_occurrences_max - 1;
                    END IF;

                  END LOOP;

                IF this_person_procedure_count >= this_target_procedure_count THEN
                  --IF normal_rand(1, 0, 1) <= 0.8 THEN
                    EXIT cond_loop;
                  --END IF;
                END IF;

              END IF;

            END IF;

          END LOOP;

        END LOOP;
      END;

      --Prevent Deadlock
      IF random() <= 0.2 THEN
        this_target_procedure_count := this_target_procedure_count - 1;
      END IF;

    END LOOP;

--     PERFORM insert_log('Simulated procedure',
--         'ins_sim_procedure');

  END;
  $$ LANGUAGE plpgsql;