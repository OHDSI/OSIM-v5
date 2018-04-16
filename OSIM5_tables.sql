--================================================================================
-- POSTGRES PACKAGE OSIM5 v2.1.000
-- Person Condition Drug Simulator
--
-- Oracle standard TABLE definitions 
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
--    08 Dec 2010
--
--    Oracle PL/SQL Package for creation and selection of era cohorts from
--    a CDM database.
--
--    ©2011 Foundation for the National Institutes of Health (FNIH)
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
--  v1.5.002 - 10 Feb 2011 - R Murray (UBC)      - Added Drug Eras Count to General Table
--  v2.1.000 - 27 Mar 2018 - K Mukadam (GTRI)    - Modified code for OMOP CDM v5 and converted to PostgreSQL
--================================================================================


--================================================================================
-- SIMULATED TABLES
--================================================================================

--================================================================================
-- TABLE osim_condition_era
--================================================================================
SET search_path TO synthetic_data_generation_mimic;

DROP TABLE IF EXISTS osim_condition_era;
CREATE UNLOGGED TABLE osim_condition_era (
  condition_era_id NUMERIC(15, 0) NOT NULL,
  person_id NUMERIC(12, 0) NOT NULL,
  condition_concept_id NUMERIC(15, 0),
  condition_era_start_date DATE,
  condition_era_end_date DATE,
  condition_occurrence_count NUMERIC(5, 0)
) WITH (OIDS= TRUE, FILLFACTOR = 90); --opposite of PCTFREE


CREATE INDEX xn_cond_era_concept_id ON osim_condition_era (condition_concept_id ASC)
WITH (FILLFACTOR = 90);

CREATE INDEX xn_cond_era_person_id ON osim_condition_era (person_id ASC) 
WITH (FILLFACTOR = 90);

CREATE INDEX xn_cond_era_start_date ON osim_condition_era (condition_era_start_date ASC)
WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_observation_period
--================================================================================
DROP TABLE IF EXISTS osim_observation_period;
CREATE UNLOGGED TABLE osim_observation_period (
  observation_period_id NUMERIC(15, 0) NOT NULL,
  person_id NUMERIC(12, 0) NOT NULL,
  observation_period_start_date DATE,
  observation_period_end_date DATE,
  period_type_concept_id NUMERIC(15, 0),
  rx_data_availability TEXT,
  dx_data_availability TEXT,
  hospital_data_availability TEXT
)
WITH (OIDS= TRUE, FILLFACTOR = 90);
--COMPRESS;

CREATE INDEX xn_obs_period_person_id ON osim_observation_period (person_id ASC) 
WITH (FILLFACTOR = 90);
 
CREATE INDEX xn_obs_period_start_date ON osim_observation_period (observation_period_start_date ASC) 
WITH (FILLFACTOR = 90);
 
--================================================================================
-- TABLE osim_person
--================================================================================
DROP TABLE IF EXISTS osim_person;
CREATE UNLOGGED TABLE osim_person (
  person_id NUMERIC(12, 0) NOT NULL,
  year_of_birth NUMERIC(4, 0),
  gender_concept_id NUMERIC(8, 0),
  race_concept_id NUMERIC(8, 0),
  ethnicity_concept_id NUMERIC(8, 0),
  location_id NUMERIC(8, 0),
  gender_source_concept_id NUMERIC(8, 0),
  race_source_concept_id NUMERIC(8, 0),
  ethnicity_source_concept_id NUMERIC(8, 0)
)
WITH (OIDS= TRUE, FILLFACTOR = 90);

CREATE UNIQUE INDEX xpk_person_person_id ON osim_person (person_id ASC)
 WITH (FILLFACTOR = 90);


--================================================================================
-- TABLE osim_drug_era
--================================================================================
DROP TABLE IF EXISTS osim_drug_era CASCADE;
CREATE UNLOGGED TABLE osim_drug_era
(
  drug_era_id NUMERIC(15, 0) NOT NULL,
  drug_era_start_date DATE,
  drug_era_end_date DATE,
  person_id NUMERIC(12, 0) NOT NULL,
  drug_concept_id NUMERIC(15, 0),
  drug_exposure_count NUMERIC(5, 0)
)
WITH (OIDS= TRUE, FILLFACTOR = 90);
--COMPRESS;

CREATE INDEX xn_drug_era_concept_id ON osim_drug_era (drug_concept_id ASC)
  WITH (FILLFACTOR = 90);
CREATE INDEX xn_drug_era_person_id ON osim_drug_era (person_id ASC)
  WITH (FILLFACTOR = 90);
CREATE INDEX xn_drug_era_start_date ON osim_drug_era (drug_era_start_date ASC)
  WITH (FILLFACTOR = 90);
   
--================================================================================
-- TABLE osim_drug_outcome
--================================================================================
DROP TABLE IF EXISTS osim_drug_outcome;
CREATE UNLOGGED TABLE osim_drug_outcome (
  risk_or_benefit VARCHAR(7) NOT NULL,
  drug_concept_id NUMERIC(15, 0) NOT NULL,
  condition_concept_id NUMERIC(15, 0) NOT NULL,
  relative_risk NUMERIC(8, 8) NOT NULL,
  outcome_risk_type VARCHAR(20) NOT NULL,
  outcome_onset_days_min NUMERIC(8, 0) NOT NULL,
  outcome_onset_days_max NUMERIC(8, 0)
)
WITH (OIDS= TRUE, FILLFACTOR = 90);

--================================================================================
-- TABLE osim_tmp_outcome
--================================================================================
DROP TABLE IF EXISTS osim_tmp_outcome;
CREATE GLOBAL TEMPORARY TABLE osim_tmp_outcome (
  person_id NUMERIC(12, 0) NOT NULL,
  drug_era_id NUMERIC(12, 0) NOT NULL,
  condition_era_id NUMERIC(12, 0) NOT NULL
) ON COMMIT DELETE ROWS;

--================================================================================
-- TABLE osim_tmp_condition_era
--================================================================================
DROP TABLE IF EXISTS osim_tmp_condition_era;
CREATE GLOBAL TEMPORARY TABLE osim_tmp_condition_era (
  condition_era_id NUMERIC(15, 0) NOT NULL,
  condition_era_start_date DATE,
  person_id NUMERIC(12, 0) NOT NULL,
  confidence NUMERIC,
  condition_era_end_date DATE,
  condition_concept_id NUMERIC(15, 0),
  condition_occurrence_count NUMERIC(5, 0)
) ON COMMIT DELETE ROWS;

--================================================================================
-- TABLE osim_tmp_drug_era
--================================================================================
DROP TABLE IF EXISTS osim_tmp_drug_era;
CREATE GLOBAL TEMPORARY TABLE osim_tmp_drug_era (
  drug_era_start_date DATE,
  drug_era_end_date DATE,
  person_id NUMERIC(12, 0) NOT NULL,
  drug_exposure_type VARCHAR(3) NOT NULL,
  drug_concept_id NUMERIC(15, 0),
  drug_exposure_count NUMERIC(5, 0)
) ON COMMIT DELETE ROWS;


--================================================================================
-- TABLE osim_person_condition
--================================================================================
DROP TABLE IF EXISTS osim_person_condition;
CREATE GLOBAL TEMPORARY TABLE osim_person_condition (
  person_id             NUMERIC(12, 0) NOT NULL,
  condition_concept_id  NUMERIC(12, 0) NOT NULL
) WITH OIDS;

--================================================================================
-- TABLE osim_log
--================================================================================
DROP TABLE IF EXISTS osim_log;
CREATE UNLOGGED TABLE osim_log (
  log_date              TIMESTAMP DEFAULT LOCALTIMESTAMP NOT NULL,
  stored_procedure_name VARCHAR(50),
  MESSAGE               VARCHAR(500) NOT NULL
) WITH OIDS;

--================================================================================
-- PROBABILTY TABLES
--================================================================================
--================================================================================
-- TABLE osim_src_db_attributes
--================================================================================
DROP TABLE IF EXISTS osim_src_db_attributes;
CREATE UNLOGGED TABLE osim_src_db_attributes (
  db_min_date DATE, 
  db_max_date DATE, 
  persons_count NUMERIC(15,0), 
  condition_eras_count NUMERIC(15,0),
  drug_eras_count NUMERIC(15,0)
) WITH OIDS;
 
--================================================================================
-- TABLE osim_gender_probability
--================================================================================
DROP TABLE IF EXISTS osim_gender_probability;
CREATE UNLOGGED TABLE osim_gender_probability (
	gender_concept_id NUMERIC(15,0), 
  n NUMERIC(10,0), 
	accumulated_probability FLOAT
) WITH OIDS;

--================================================================================
-- TABLE osim_age_at_obs_probability
--================================================================================
DROP TABLE IF EXISTS osim_age_at_obs_probability;
  CREATE UNLOGGED TABLE osim_age_at_obs_probability (
  gender_concept_id NUMERIC(15,0), 
	age_at_obs NUMERIC(15,0), 
  n NUMERIC(10,0), 
	accumulated_probability FLOAT
) WITH OIDS;

--================================================================================
-- TABLE osim_cond_count_probability
--================================================================================
DROP TABLE IF EXISTS osim_cond_count_probability;
  CREATE UNLOGGED TABLE osim_cond_count_probability (
  gender_concept_id NUMERIC(15,0), 
	age_at_obs NUMERIC(15,0), 
  cond_era_count NUMERIC(8,0),
	cond_concept_count NUMERIC(5,0), 
  n NUMERIC(10,0), 
	accumulated_probability FLOAT
) WITH OIDS;

--================================================================================
-- TABLE osim_time_obs_probability
--================================================================================
DROP TABLE IF EXISTS osim_time_obs_probability;
  CREATE UNLOGGED TABLE osim_time_obs_probability (
  gender_concept_id NUMERIC(15,0), 
	age_at_obs NUMERIC(15,0), 
	cond_count_bucket NUMERIC(5,0), 
  time_observed NUMERIC(3,0),
  n NUMERIC(10,0), 
	accumulated_probability FLOAT
) WITH OIDS;

--================================================================================
-- TABLE osim_cond_era_count_prob
--================================================================================
DROP TABLE IF EXISTS osim_cond_era_count_prob;
  CREATE UNLOGGED TABLE osim_cond_era_count_prob (
  condition_concept_id NUMERIC(15,0), 
	cond_count_bucket NUMERIC(5,0), 
  time_remaining NUMERIC(3,0),
	cond_era_count NUMERIC(5,0), 
  n NUMERIC(10,0), 
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);
 
CREATE INDEX osim_cond_era_count_ix1 ON osim_cond_era_count_prob (
  condition_concept_id, cond_count_bucket, time_remaining) 
  WITH (FILLFACTOR = 90);
 
CREATE INDEX osim_cond_era_count_ix2 ON osim_cond_era_count_prob (accumulated_probability) 
  WITH (FILLFACTOR = 90);
 
--================================================================================
-- TABLE osim_first_cond_probability
--================================================================================
DROP TABLE IF EXISTS osim_first_cond_probability;
  CREATE UNLOGGED TABLE osim_first_cond_probability (
  gender_concept_id NUMERIC(15,0), 
	age_range NUMERIC(3,0), 
	cond_count_bucket NUMERIC(4,0), 
  time_remaining NUMERIC(3,0),
	condition1_concept_id NUMERIC(15,0), 
	condition2_concept_id NUMERIC(15,0), 
	delta_days FLOAT,
  n NUMERIC(10,0), 
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);
 
CREATE INDEX osim_first_cond_ix1 ON osim_first_cond_probability (
  condition1_concept_id, age_range, gender_concept_id, cond_count_bucket, time_remaining) 
  WITH (FILLFACTOR = 90);
    
CREATE INDEX osim_first_cond_ix2 ON osim_first_cond_probability (accumulated_probability)
  WITH (FILLFACTOR = 90);
    
    
--================================================================================
-- TABLE osim_cond_reoccur_probability
--================================================================================
DROP TABLE IF EXISTS osim_cond_reoccur_probability;
  CREATE UNLOGGED TABLE osim_cond_reoccur_probability (
	condition_concept_id NUMERIC(15,0), 
	age_range NUMERIC(3,0), 
	time_remaining NUMERIC(4,0), 
	delta_days FLOAT,
  n NUMERIC(10,0), 
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);
 
CREATE INDEX osim_cond_reoccur_ix1 ON osim_cond_reoccur_probability (
  condition_concept_id, age_range, time_remaining) 
  WITH (FILLFACTOR = 90);
    
CREATE INDEX osim_cond_reoccur_ix2 ON osim_cond_reoccur_probability (accumulated_probability)
  WITH (FILLFACTOR = 90);
 

--================================================================================
-- TABLE osim_drug_count_prob
--================================================================================
DROP TABLE IF EXISTS osim_drug_count_prob;
CREATE UNLOGGED TABLE osim_drug_count_prob(
  gender_concept_id NUMERIC(15,0),
  age_bucket NUMERIC(5,0),
  condition_count_bucket NUMERIC(5,0),
  drug_count NUMERIC(5,0),
  n NUMERIC(10,0), 
	accumulated_probability FLOAT
) WITH OIDS;

CREATE INDEX osim_drug_count_prob_ix1 ON osim_drug_count_prob (
  gender_concept_id, age_bucket, condition_count_bucket) 
  WITH (FILLFACTOR = 90);
    
CREATE INDEX osim_drug_count_prob_ix2 ON osim_drug_count_prob (accumulated_probability)
  WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_cond_drug_count_prob
--================================================================================
DROP TABLE IF EXISTS osim_cond_drug_count_prob;
CREATE UNLOGGED TABLE osim_cond_drug_count_prob(
  condition_concept_id NUMERIC(15,0),
  age_bucket NUMERIC(5,0),
  interval_bucket NUMERIC(5,0),
  drug_count_bucket NUMERIC(5,0),
  condition_count_bucket NUMERIC(5,0),
  drug_count NUMERIC(5,0),
  n NUMERIC(10,0), 
	accumulated_probability FLOAT
) WITH OIDS;

CREATE INDEX osim_cond_drug_count_prob_ix1 ON osim_cond_drug_count_prob (
  condition_concept_id, age_bucket, interval_bucket, drug_count_bucket, 
  condition_count_bucket) 
  WITH (FILLFACTOR = 90);
    
CREATE INDEX osim_cond_drug_count_prob_ix2 ON osim_cond_drug_count_prob (
  accumulated_probability)
  WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_cond_procedure_count_prob
--================================================================================
DROP TABLE IF EXISTS osim_cond_procedure_count_prob;
CREATE UNLOGGED TABLE osim_cond_procedure_count_prob(
  condition_concept_id NUMERIC(15,0),
  age_bucket NUMERIC(5,0),
  interval_bucket NUMERIC(5,0),
  procedure_count_bucket NUMERIC(5,0),
  condition_count_bucket NUMERIC(5,0),
  procedure_count NUMERIC(5,0),
  n NUMERIC(10,0),
	accumulated_probability FLOAT
) WITH OIDS;

CREATE INDEX osim_cond_procedure_count_prob_ix1 ON osim_cond_procedure_count_prob (
  condition_concept_id, age_bucket, interval_bucket, procedure_count_bucket,
  condition_count_bucket)
  WITH (FILLFACTOR = 90);

CREATE INDEX osim_cond_procedure_count_prob_ix2 ON osim_cond_procedure_count_prob (
  accumulated_probability)
  WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_cond_first_drug_prob
--================================================================================
DROP TABLE IF EXISTS osim_cond_first_drug_prob;
CREATE UNLOGGED TABLE osim_cond_first_drug_prob(
  condition_concept_id NUMERIC(15,0),
  interval_bucket NUMERIC(5,0),  
  gender_concept_id NUMERIC(15,0),
  age_bucket NUMERIC(5,0),
  condition_count_bucket NUMERIC(5,0),
  drug_count_bucket NUMERIC(5,0),
  day_cond_count NUMERIC(5,0),
  drug_concept_id NUMERIC(15,0),
  delta_days NUMERIC(5,0),
  n NUMERIC(10,0), 
	accumulated_probability FLOAT
) WITH OIDS;

CREATE INDEX osim_cond_drug_prob_ix1 ON osim_cond_first_drug_prob (
  condition_concept_id, interval_bucket, age_bucket, condition_count_bucket,
  drug_count_bucket, day_cond_count, gender_concept_id) 
  WITH (FILLFACTOR = 90);
    
CREATE INDEX osim_cond_drug_prob_ix2 ON osim_cond_first_drug_prob (accumulated_probability)
  WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_cond_first_procedure_prob
--================================================================================
DROP TABLE IF EXISTS osim_cond_first_procedure_prob;
CREATE UNLOGGED TABLE osim_cond_first_procedure_prob(
  condition_concept_id NUMERIC(15,0),
  interval_bucket NUMERIC(5,0),
  gender_concept_id NUMERIC(15,0),
  age_bucket NUMERIC(5,0),
  condition_count_bucket NUMERIC(5,0),
  procedure_count_bucket NUMERIC(5,0),
  day_cond_count NUMERIC(5,0),
  procedure_concept_id NUMERIC(15,0),
  delta_days NUMERIC(5,0),
  n NUMERIC(10,0),
	accumulated_probability FLOAT
) WITH OIDS;

CREATE INDEX osim_cond_procedure_prob_ix1 ON osim_cond_first_procedure_prob (
  condition_concept_id, interval_bucket, age_bucket, condition_count_bucket,
  procedure_count_bucket, day_cond_count, gender_concept_id)
  WITH (FILLFACTOR = 90);

CREATE INDEX osim_cond_procedure_prob_ix2 ON osim_cond_first_procedure_prob (accumulated_probability)
  WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_drug_era_count_prob
--================================================================================
DROP TABLE IF EXISTS osim_drug_era_count_prob;
  CREATE UNLOGGED TABLE osim_drug_era_count_prob (
  drug_concept_id NUMERIC(15,0), 
	drug_count_bucket NUMERIC(5,0), 
  condition_count_bucket NUMERIC(5,0), 
  age_range NUMERIC(3,0),
  time_remaining NUMERIC(3,0),
	drug_era_count NUMERIC(5,0), 
  total_exposure NUMERIC(5,0), 
  n NUMERIC(10,0), 
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);
 
CREATE INDEX osim_drug_era_count_ix1 ON osim_drug_era_count_prob (
  drug_concept_id, drug_count_bucket, condition_count_bucket, age_range, time_remaining) 
  WITH (FILLFACTOR = 90);
 
CREATE INDEX osim_drug_era_count_ix2 ON osim_drug_era_count_prob (accumulated_probability) 
  WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_procedure_occurrence_count_prob
--================================================================================
DROP TABLE IF EXISTS osim_procedure_occurrence_count_prob;
  CREATE UNLOGGED TABLE osim_procedure_occurrence_count_prob (
  procedure_concept_id NUMERIC(15,0),
	procedure_count_bucket NUMERIC(5,0),
  condition_count_bucket NUMERIC(5,0),
  age_range NUMERIC(3,0),
  time_remaining NUMERIC(3,0),
	procedure_occurrence_count NUMERIC(5,0),
  total_exposure NUMERIC(5,0),
  n NUMERIC(10,0),
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);

CREATE INDEX osim_procedure_occurrence_count_ix1 ON osim_procedure_occurrence_count_prob (
  procedure_concept_id, procedure_count_bucket, condition_count_bucket, age_range, time_remaining)
  WITH (FILLFACTOR = 90);

CREATE INDEX osim_procedure_occurrence_count_ix2 ON osim_procedure_occurrence_count_prob (accumulated_probability)
  WITH (FILLFACTOR = 90);

--================================================================================
-- TABLE osim_drug_duration_probability
--================================================================================
DROP TABLE IF EXISTS osim_drug_duration_probability;
  CREATE UNLOGGED TABLE osim_drug_duration_probability (
  drug_concept_id NUMERIC(15,0), 
  time_remaining NUMERIC(3,0),
	drug_era_count NUMERIC(5,0), 
  total_exposure NUMERIC(5,0), 
  total_duration NUMERIC(5,0), 
  n NUMERIC(10,0), 
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);
 
CREATE INDEX osim_drug_duration_ix1 ON osim_drug_duration_probability (
  drug_concept_id, time_remaining, drug_era_count, total_exposure) 
  WITH (FILLFACTOR = 90);
 
CREATE INDEX osim_drug_duration_ix2 ON osim_drug_duration_probability (
  accumulated_probability) 
  WITH (FILLFACTOR = 90);

    
--================================================================================
-- TABLE osim_drug_reoccur_probability
--================================================================================
DROP TABLE IF EXISTS osim_drug_reoccur_probability;
  CREATE UNLOGGED TABLE osim_drug_reoccur_probability (
	drug_concept_id NUMERIC(15,0), 
	age_range NUMERIC(3,0), 
	time_remaining NUMERIC(4,0), 
	delta_days FLOAT,
  n NUMERIC(10,0), 
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);
 
CREATE INDEX osim_drug_reoccur_ix1 ON osim_drug_reoccur_probability (
  drug_concept_id, age_range, time_remaining) 
  WITH (FILLFACTOR = 90);
    
CREATE INDEX osim_drug_reoccur_ix2 ON osim_drug_reoccur_probability (accumulated_probability)
  WITH (FILLFACTOR = 90);
    
--================================================================================
-- TABLE osim_procedure_reoccur_probability
--================================================================================
DROP TABLE IF EXISTS osim_procedure_reoccur_probability;
  CREATE UNLOGGED TABLE osim_procedure_reoccur_probability (
	procedure_concept_id NUMERIC(15,0),
	age_range NUMERIC(3,0),
	time_remaining NUMERIC(4,0),
	delta_days FLOAT,
  n NUMERIC(10,0),
	accumulated_probability FLOAT)
  WITH (OIDS= TRUE, FILLFACTOR = 90);

CREATE INDEX osim_procedure_reoccur_ix1 ON osim_procedure_reoccur_probability (
  procedure_concept_id, age_range, time_remaining)
  WITH (FILLFACTOR = 90);

CREATE INDEX osim_procedure_reoccur_ix2 ON osim_procedure_reoccur_probability (accumulated_probability)
  WITH (FILLFACTOR = 90);

