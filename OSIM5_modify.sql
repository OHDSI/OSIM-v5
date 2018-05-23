--================================================================================
--
-- Modification of OSIM5 analysis table probabilities to provide patient cohorts
-- Based on ATLAS
-- Author: Kausar Mukadam, GTRI
-- Last Updated: May 2018
--================================================================================

SET SEARCH_PATH to synthetic_data_generation_test, public, mimic_v5;

CREATE OR REPLACE FUNCTION modify_prob()

RETURNS VOID AS $$

DECLARE

/*=========================================================================
    Initialization

    Note: Set any variable to null to remove condition
|==========================================================================
*/

 -- Initial condition that all patients must have
  CONDITION_CONCEPT       INTEGER := 375527;

 -- Interval period for patients
  INTERVAL_PERIOD         INTEGER:= 60;

 -- Age conditions
  AGE_GREATER_THAN        INTEGER:= 18;
  AGE_LESSER_THAN         INTEGER:= NULL;

 -- Drug concepts @TODO: Doesn't work, figure array lookup/sets
  DRUG_CONCEPT            INTEGER[];

BEGIN

/*=========================================================================
    Get all condition and drug concepts who's prob needs modifications
|==========================================================================
*/

CREATE TABLE IF NOT EXISTS Codesets(codeset_id int NOT NULL,
  concept_id bigint NOT NULL );

IF CONDITION_CONCEPT NOTNULL THEN
  INSERT INTO Codesets (codeset_id, concept_id)
  SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
  (
    select concept_id from mimic_v5.CONCEPT where concept_id in (CONDITION_CONCEPT)and invalid_reason is null
  UNION  select c.concept_id
    from mimic_v5.concept c
    join mimic_v5.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
    and ca.ancestor_concept_id in (CONDITION_CONCEPT)
    and c.invalid_reason is null
  ) I
  ) C;
END IF;

INSERT INTO Codesets (codeset_id, concept_id)
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(select concept_id from mimic_v5.CONCEPT where concept_id in (1140643,1154077,1116031,1189697,1189458,1118117,1103552) and invalid_reason is null
) I
) C;

/*=========================================================================
    Age conditions
|==========================================================================
*/

IF AGE_GREATER_THAN NOTNULL THEN
  UPDATE osim_age_at_obs_probability
    SET n = 0 WHERE age_at_obs < AGE_GREATER_THAN;
END IF;

IF AGE_LESSER_THAN NOTNULL THEN
  UPDATE osim_age_at_obs_probability
    SET n = 0 WHERE age_at_obs > AGE_LESSER_THAN;
END IF;

-- Recalculate accumulated prob

UPDATE osim_age_at_obs_probability AS t
  SET accumulated_probability = v_table_name.accumulated_probability
FROM (
  SELECT
  SUM(probability) OVER (PARTITION BY gender_concept_id
  ORDER BY probability ASC ROWS UNBOUNDED PRECEDING) as accumulated_probability,
  gender_concept_id, age_at_obs
  FROM
  (
    SELECT 1.0 * n / NULLIF(SUM(n) OVER(PARTITION BY gender_concept_id),0) AS probability, gender_concept_id, age_at_obs
    FROM osim_age_at_obs_probability
  ) p
) AS v_table_name
WHERE
  t.age_at_obs = v_table_name.age_at_obs
  AND
  t.gender_concept_id = v_table_name.gender_concept_id;


-- Ensure accumulated prob is still 1
UPDATE osim_age_at_obs_probability
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY gender_concept_id, age_bucket, condition_count_bucket
            ORDER BY accumulated_probability DESC)
      FROM osim_drug_count_prob)
    AND n != 0;

/*=========================================================================
    Force initial condition concept - osim_first_cond_probability
|==========================================================================
*/

IF CONDITION_CONCEPT NOTNULL THEN

  UPDATE osim_first_cond_probability
    SET n = 0
    WHERE
      condition1_concept_id = -1 -- initial condition
      AND
      condition2_concept_id NOT IN (SELECT concept_id from Codesets WHERE codeset_id = 0);

  -- Recalculate accumulated prob

  UPDATE osim_first_cond_probability AS t
    SET accumulated_probability = v_table_name.accumulated_probability
FROM (
    SELECT SUM(probability) OVER (PARTITION BY
                                  gender_concept_id, age_range, cond_count_bucket, condition1_concept_id, time_remaining
                                  ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) as accumulated_probability,
      gender_concept_id, age_range, cond_count_bucket,
      condition1_concept_id, condition2_concept_id, time_remaining
    FROM (
      SELECT 1.0 * n/ NULLIF(SUM(n) OVER
              (PARTITION BY gender_concept_id, age_range, cond_count_bucket,
                condition1_concept_id, time_remaining), 0)
              AS probability,
              gender_concept_id, age_range, cond_count_bucket, condition1_concept_id, condition2_concept_id, time_remaining
      FROM osim_first_cond_probability
      WHERE condition1_concept_id = -1
    ) p
  ) AS v_table_name
  WHERE
    t.gender_concept_id = v_table_name.gender_concept_id
    AND
    t.age_range = v_table_name.age_range
    AND
    t.cond_count_bucket = v_table_name.cond_count_bucket
    AND
    t.condition1_concept_id = v_table_name.condition1_concept_id
    AND
    t.condition2_concept_id =  v_table_name.condition2_concept_id
    AND
    t.time_remaining = v_table_name.time_remaining;

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
        FROM osim_first_cond_probability)
      AND n != 0;

      UPDATE osim_first_cond_probability
          SET accumulated_probability = 0
          WHERE oid IN
           (SELECT DISTINCT
              FIRST_VALUE(oid)
                OVER
                 (PARTITION BY condition1_concept_id, age_range, gender_concept_id,
                    cond_count_bucket, time_remaining
                  ORDER BY accumulated_probability DESC)
            FROM osim_first_cond_probability)
          AND n = 0;
END IF;

END;
$$ LANGUAGE plpgsql;
