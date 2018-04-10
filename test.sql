/*
Running the service
1. Run OSIM5_tables.sql: Creates all necessary tables for code

2. Run OSIM5_views.sql: Creates necessary views based on the database.
Check OSIM5_views.sql file to edit the database schema as required

3. Run OSIM5_package.sql: Defines functions used in this script, which perform synthetic data generation.

4. Analyze the database: Run first query in this script

5. Generate synthetic data: Run query 2 in this script
*/

SET SEARCH_PATH TO synthetic_data_generation, public;

-- Step 4. Analyze database
DO $$
BEGIN
    PERFORM ins_gender_probability();
END$$;

DO $$
BEGIN
    PERFORM ins_age_at_obs_probability();
END$$;

DO $$
BEGIN
    PERFORM ins_cond_count_probability();
END$$;

DO $$
BEGIN
    PERFORM ins_time_obs_probability();
END$$;

DO $$
BEGIN
    PERFORM ins_cond_era_count_prob();
END$$;

DO $$
BEGIN
   PERFORM ins_cond_days_before_prob();
END$$;

DO $$
BEGIN
   PERFORM ins_drug_count_prob();
END$$;

DO $$
BEGIN
   PERFORM ins_cond_drug_count_prob();
END$$;

DO $$
BEGIN
   PERFORM ins_cond_first_drug_prob();
END$$;

DO $$
BEGIN
   PERFORM ins_drug_era_count_prob();
END$$;

DO $$
BEGIN
   PERFORM ins_drug_duration_probability();
END$$;

DO $$
BEGIN
    PERFORM ins_first_cond_probability();
END$$;

-- -- Step 5. Generate synthetic data
-- DO $$
-- BEGIN
--     PERFORM ins_sim_data(1000,0); -- number of patients, start_id
-- END $$;
