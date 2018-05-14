

INSERT INTO omop.observation_period
   select row_number() OVER (ORDER BY combined.person_id), combined.person_id,
     MIN(combined.start_date), GREATEST(MAX(combined.start_date), MAX(combined.end_date)), 44814722
   FROM (
          SELECT person_id, condition_start_date as start_date, condition_end_date as end_date from omop.condition_occurrence
          where person_id > 10000000 AND person_id <= 20000000
          UNION ALL
          SELECT person_id, drug_exposure_start_date as start_date, drug_exposure_end_date as end_date from omop.drug_exposure
          where person_id > 10000000 AND person_id <= 20000000
          UNION ALL
          SELECT person_id, procedure_date as start_date, procedure_date as end_date from omop.procedure_occurrence
          where person_id > 10000000 AND person_id <= 20000000
          UNION ALL
          SELECT person_id, observation_date as start_date, observation_date as end_date from omop.observation
          where person_id > 10000000 AND person_id <= 20000000
          UNION ALL
          SELECT  person_id, measurement_date as start_date, measurement_date as end_date from omop.measurement
          where person_id > 10000000 AND person_id <= 20000000
          UNION ALL
          SELECT person_id, visit_start_date as start_date, visit_end_date as end_date from omop.visit_occurrence
          where person_id > 10000000 AND person_id <= 20000000
        ) combined
   GROUP BY combined.person_id;