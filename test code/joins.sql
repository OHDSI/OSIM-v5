
--===============================================
--
-- Inner join of conditions, procedures and drugs with concept table for better readability
--
--===============================================

SET SEARCH_PATH to synthetic_data_generation, public;

--
-- Drugs
--
SELECT *
INTO osim_drug_era_concept
FROM
  osim_drug_era t1 INNER JOIN "omop"."concept" t2
  ON t1."drug_concept_id" = t2."concept_id";

--
--Conditions
--
SELECT *
INTO osim_condition_era_concept
FROM
  osim_condition_era t1 INNER JOIN "omop"."concept" t2
  ON t1."condition_concept_id" = t2."concept_id";

--
-- Procedures
--
SELECT *
INTO osim_procedure_occurrence_concept
FROM
  osim_procedure_occurrence t1 INNER JOIN "omop"."concept" t2
  ON t1."procedure_concept_id" = t2."concept_id";


