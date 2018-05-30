--================================================================================
-- POSTGRES PACKAGE OSIM5 v2.1.000
-- Person Condition Drug Simulator
--
-- Views (of CDM tables) must be created first for:
--   s_person             for person
--   s_observation_period for observation_period
--   s_condition_era      for condition_era
--   s_drug_era           for drug_era
-- Edit first 4 views in this file with correct path to schema
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
--    08 Dec 2010
--
--    Oracle PL/SQL Package for creation and selection of era cohorts from
--    a CDM database.
--
--    �2011 Foundation for the National Institutes of Health (FNIH)
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
--
-- CHANGE LOG
--  v1.0.000 - 17 May 2010 - R Murray (ProSanos) - Original Version
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
--  V2.1.000 - 27 Mar 2018 - K Mukadam (GTRI)    - Modified code for OMOP CDM v5 and converted to PostgreSQL
--
-- Follow steps 1 & 2 to edit this file as needed

--================================================================================
-- Step 1. Adjust search path to destination schema as necessary
SET search_path TO synthetic_data_generation, public;

--================================================================================
-- Set 2. Edit views/schema to point to source tables
-- currently only selects persons with at least one condition in the era table

CREATE OR REPLACE VIEW s_person as select * from omop.person
    where person_id in (select condition_era.person_id from omop.condition_era);
CREATE OR REPLACE VIEW s_observation_period as select * from omop.observation_period
    where person_id in (select condition_era.person_id from omop.condition_era);
CREATE OR REPLACE VIEW s_condition_era as select * from omop.condition_era;
CREATE OR REPLACE VIEW s_drug_era as select * from omop.drug_era;
CREATE OR REPLACE VIEW s_procedure_occurrence as select * from omop.procedure_occurrence;
--================================================================================
-- VIEW v_src_person
--================================================================================
CREATE OR REPLACE VIEW v_src_person
 (person_id, year_of_birth, gender_concept_id, race_concept_id, ethnicity_concept_id,
  location_id, gender_source_concept_id,race_source_concept_id, ethnicity_source_concept_id) AS
SELECT /*+ NO_PARALLEL(person) */DISTINCT
  person.person_id, person.year_of_birth, person.gender_concept_id, person.race_concept_id, person.ethnicity_concept_id,
  person.location_id, person.gender_source_concept_id, person.race_source_concept_id, ethnicity_source_concept_id
FROM  s_person person
INNER JOIN s_observation_period period on person.person_id = period.person_id
WHERE person.year_of_birth IS NOT NULL AND period.observation_period_start_date IS NOT NULL;
--WITH READ ONLY;

--================================================================================
-- VIEW v_src_person_strata
--================================================================================
CREATE OR REPLACE VIEW v_src_person_strata
 (person_id, year_of_birth, gender_concept_id, race_concept_id, ethnicity_concept_id,
  location_id, gender_source_concept_id, race_source_concept_id, ethnicity_source_concept_id,
  observation_period_start_date, observation_period_end_date, age,
  obs_duration_days, condition_concepts,drug_concepts
  --,source_person_key, source_location_code,
) AS
WITH drug_counts AS
 (SELECT
    person.person_id,
    COUNT (DISTINCT drug_concept_id) AS drugs
  FROM v_src_person person
  INNER JOIN s_drug_era drug_era ON person.person_id = drug_era.person_id
  GROUP BY person.person_id),
cond_counts AS
 (SELECT
    person.person_id,
    COUNT (DISTINCT condition_concept_id) AS conditions
  FROM v_src_person person
  INNER JOIN s_condition_era condition_era ON person.person_id = condition_era.person_id
  GROUP BY person.person_id),
  procedure_counts AS
 (SELECT
    person.person_id,
    COUNT (DISTINCT procedure_concept_id) AS procedures
  FROM v_src_person person
  INNER JOIN s_procedure_occurrence procedure_occurence ON person.person_id = procedure_occurence.person_id
  GROUP BY person.person_id),

person_strata AS
 (SELECT /*+ NO_PARALLEL (person) */ 
    person.person_id, person.year_of_birth, person.gender_concept_id, person.race_concept_id, person.ethnicity_concept_id,
    person.location_id,  person.gender_source_concept_id, person.race_source_concept_id, person.ethnicity_source_concept_id,
    MIN(period.observation_period_start_date) AS observation_period_start_date,
    MAX(period.observation_period_end_date) AS observation_period_end_date,
    TO_NUMBER(TO_CHAR(MIN(period.observation_period_start_date),'yyyy'), '9999') - person.year_of_birth as age,
    MAX(period.observation_period_end_date) - MIN(period.observation_period_start_date) AS obs_duration_days
  FROM v_src_person person
  INNER JOIN s_observation_period period on person.person_id = period.person_id
  GROUP BY   person.person_id, person.year_of_birth, person.gender_concept_id, person.race_concept_id, person.ethnicity_concept_id,
    person.location_id, person.gender_source_concept_id, person.race_source_concept_id, person.ethnicity_source_concept_id)
SELECT strata.person_id, strata.year_of_birth, strata.gender_concept_id, strata.race_concept_id, strata.ethnicity_concept_id,
  strata.location_id, strata.gender_source_concept_id, strata.race_source_concept_id, strata.ethnicity_source_concept_id,
  strata.observation_period_start_date, strata.observation_period_end_date, strata.age, strata.obs_duration_days,
  coalesce(cond.conditions,0) AS condition_concepts,
  coalesce(drug.drugs,0) AS drug_concepts,
  coalesce(procedure.procedures,0) AS procedure_concepts
FROM person_strata strata
  LEFT JOIN cond_counts cond ON strata.person_id = cond.person_id 
  LEFT JOIN drug_counts drug ON strata.person_id = drug.person_id
  LEFT JOIN procedure_counts procedure ON strata.person_id = procedure.person_id;

--================================================================================
-- VIEW v_observation_period
--================================================================================
CREATE OR REPLACE VIEW v_src_observation_period
 (observation_period_id,   observation_period_start_date, observation_period_end_date,
  person_id
-- , person_status_concept_id, rx_data_availability, dx_data_availability, hospital_data_availability, confidence
) AS
SELECT /*+ NO_PARALLEL(obs) */ obs.observation_period_id,
  obs.observation_period_start_date,
  obs.observation_period_end_date,
  obs.person_id
  --,obs.person_status_concept_id, obs.rx_data_availability, obs.dx_data_availability, obs.hospital_data_availability, obs.confidence
FROM s_observation_period obs
INNER JOIN v_src_person person ON obs.person_id = person.person_id;

--================================================================================
-- VIEW v_src_condition_era1_ids
-- for CDM data with 0 day persistence window condition eras
--================================================================================
CREATE OR REPLACE VIEW v_src_condition_era1_ids
 (condition_occurrence_id, condition_occurrence_count) AS
SELECT /*+ NO_PARALLEL(cond) */ DISTINCT
  condition_era_id, cond.condition_occurrence_count
FROM s_condition_era cond
INNER JOIN v_src_person person ON cond.person_id = person.person_id;

--================================================================================
-- VIEW v_src_condition_era1
-- for CDM data with 0 day persistence window condition eras
--================================================================================
CREATE OR REPLACE VIEW v_src_condition_era1
  (condition_era_id, condition_era_start_date, person_id, condition_era_end_date, 
   condition_concept_id, condition_occurrence_count) AS
SELECT /*+ NO_PARALLEL(cond) */
  condition_era_id, condition_era_start_date, cond.person_id, condition_era_end_date, 
  condition_concept_id, condition_occurrence_count
FROM s_condition_era cond
INNER JOIN v_src_person person ON cond.person_id = person.person_id;


--================================================================================
-- VIEW v_src_first_conditions
-- retrieves very first condition era of each condition concept for each person
--================================================================================
CREATE OR REPLACE VIEW v_src_first_conditions
(person_id, condition_era_start_date, condition_concept_id) AS 
SELECT DISTINCT
  cond.person_id,
  FIRST_VALUE(condition_era_start_date)
    OVER 
    (PARTITION BY cond.person_id, cond.condition_concept_id
      ORDER BY cond.condition_era_start_date) AS condition_era_start_date,
  condition_concept_id
FROM v_src_condition_era1 cond
INNER JOIN s_person person  ON cond.person_id = person.person_id
GROUP BY cond.person_id, condition_era_start_date,  condition_concept_id;

--================================================================================
-- VIEW v_all_conditions
-- retrieves all condition era for each person
--================================================================================   
CREATE OR REPLACE VIEW v_src_all_conditions
(person_id, gender_concept_id, age, condition_era_start_date, condition_concept_id) AS 
  -- actual condition
  SELECT DISTINCT
    person.person_id,
    gender_concept_id, 
    TO_NUMBER(TO_CHAR(condition_era_start_date,'yyyy'), '9999') - year_of_birth AS age,
    condition_era_start_date, 
    condition_concept_id
  FROM v_src_condition_era1 cond
  INNER JOIN v_src_person person ON cond.person_id = person.person_id;
   
--================================================================================
-- VIEW v_src_drug_era1
-- for CDM data with 0 day persistence window drug eras
--================================================================================
CREATE OR REPLACE VIEW v_src_drug_era1
  (drug_era_id, drug_era_start_date, drug_era_end_date, person_id, 
   drug_concept_id, drug_exposure_count) AS
SELECT /*+ NO_PARALLEL(drug) */
  drug_era_id, drug_era_start_date, drug_era_end_date, drug.person_id,
  drug_concept_id, drug_exposure_count
FROM s_drug_era drug
INNER JOIN v_src_person person ON drug.person_id = person.person_id;

--================================================================================
-- VIEW v_src_first_drugs
-- retrieves very first drug era of each drug concept for each person
--================================================================================
CREATE OR REPLACE VIEW v_src_first_drugs
(person_id, drug_era_start_date, drug_era_end_date, drug_concept_id) AS 
SELECT DISTINCT
  drug.person_id,
  FIRST_VALUE(drug_era_start_date)
    OVER 
    (PARTITION BY drug.person_id, drug.drug_concept_id
      ORDER BY drug.drug_era_start_date) AS drug_era_start_date,
  FIRST_VALUE(drug_era_end_date)
    OVER 
    (PARTITION BY drug.person_id, drug.drug_concept_id
      ORDER BY drug.drug_era_start_date) AS drug_era_end_date,
  drug_concept_id
FROM v_src_drug_era1 drug
INNER JOIN s_person person ON drug.person_id = person.person_id
GROUP BY drug.person_id, drug_era_start_date,  drug_era_end_date, drug_concept_id;

--================================================================================
-- VIEW v_src_drug_era1_ids
-- for CDM data with 0 day persistence window drug eras
--================================================================================
CREATE OR REPLACE VIEW v_src_drug_era1_ids
 (drug_exposure_id, drug_exposure_count) AS
SELECT /*+ NO_PARALLEL(cond) */ DISTINCT
  drug_era_id, drug.drug_exposure_count
FROM s_drug_era drug
INNER JOIN v_src_person person ON drug.person_id = person.person_id;



--==============================================================
--PROCEDURES
--==============================================================

--================================================================================
-- VIEW v_src_procedure_occurrence1
--================================================================================
CREATE OR REPLACE VIEW v_src_procedure_occurrence1
  (procedure_occurrence_id, procedure_date, person_id,
  procedure_concept_id, quantity) AS
SELECT /*+ NO_PARALLEL(drug) */
  procedure_occurrence_id, procedure_date, p.person_id,
  procedure_concept_id, quantity
FROM s_procedure_occurrence p
INNER JOIN v_src_person person ON p.person_id = person.person_id;

--================================================================================
-- VIEW v_src_first_procedures
-- retrieves very first procedure occurrence of each procedure concept for each person
--================================================================================
CREATE OR REPLACE VIEW v_src_first_procedures
(person_id, procedure_date, procedure_concept_id) AS
SELECT DISTINCT
  procedure.person_id,
  FIRST_VALUE(procedure_date)
    OVER
    (PARTITION BY procedure.person_id, procedure.procedure_concept_id
      ORDER BY procedure.procedure_date) AS procedure_date,
  procedure_concept_id
FROM v_src_procedure_occurrence1 procedure
INNER JOIN s_person person ON procedure.person_id = person.person_id
GROUP BY procedure.person_id, procedure_date, procedure_concept_id;

--================================================================================
-- VIEW v_src_procedure_occurrence1_ids
--================================================================================
CREATE OR REPLACE VIEW v_src_procedure_occurrence1_ids
 (procedure_occurrence_id, quantity) AS
SELECT /*+ NO_PARALLEL(cond) */ DISTINCT
  procedure_occurrence_id, procedure.quantity
FROM s_procedure_occurrence procedure
INNER JOIN v_src_person person ON procedure.person_id = person.person_id;

