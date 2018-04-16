/*
========================================================================================
Running the service
========================================================================================

1. Run OSIM5_tables.sql: Creates all necessary tables for code

2. Run OSIM5_views.sql: Creates necessary views based on the database
    (Check OSIM5_views.sql file to edit the database schema as required)

3. Run OSIM5_package.sql: Defines functions used in this script,
    which perform synthetic data generation.

4. Analyze the database: Run first query in this script

5. Generate synthetic data: Run query 2 in this script
==========================================================================================
*/


-- Set search_path to schema where you want to generate the synthetic data
-- Make sure to run the previous files (tables, views) in the same schema as above
-- Else comment this section

SET SEARCH_PATH TO synthetic_data_generation, public;

-- Step 4. Analyze database
DO $$
BEGIN
    PERFORM analyze_source_db();
END$$;


-- -- Step 5. Generate synthetic data
DO $$
BEGIN
    -- number of patients, start_id
    PERFORM ins_sim_data(1000,0);
END $$;
