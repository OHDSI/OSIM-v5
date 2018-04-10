SELECT *
INTO osim_drug_era_concept
FROM
  osim_drug_era t1 INNER JOIN "mimic_v5"."concept" t2
  ON t1."drug_concept_id" = t2."concept_id";

SELECT *
INTO osim_condition_era_concept
FROM
  osim_condition_era t1 INNER JOIN "mimic_v5"."concept" t2
  ON t1."condition_concept_id" = t2."concept_id";




