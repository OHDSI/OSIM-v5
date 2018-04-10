#*******************************************************************************
#
#   OSIM5_package.R - version 1.0.001
#
#   Observational Medical Outcomes Partnership
#
#   Procedure for constructing simulated observational datasets
#
#   Initially Written in Oracle PL/SQL: Rich Murray, United BioSource Corporation
#   Last modified: 15 February 2011
#   �2010 Foundation for the National Institutes of Health
#
#   Written in PostgreSQL: Kausar Mukadam, Georgia Tech Research Institute
#
#   DESCRIPTION:
#
#       OSIM 5 and OSIM 2 is an automated procedure to construct simulated datasets to
#       supplement methods evaluation for identifying drug-outcome associations 
#       in observational data sources.  The simulated datasets produced by this 
#       program are modeled after real observational data sources, but are 
#       comprised of hypothetical persons with simulated drug exposures and 
#       health outcomes occurrences. Hypothetical persons are created with
#       drug exposure periods and instances of conditions based on random sampling 
#       from probability distributions and transitional probabilities that define
#       the relationships between the actual conditions and drugs. 
#
#       The relationships created within the simulated datasets mirror automatically 
#       analyzed relationships from a real observational data source.
#
#       To accommodate the efficient and timely generation of a large number of 
#       simulated records using a variety of high performance and lower-end 
#       platforms, the program has been designed for both local and distributed 
#       execution modes.  In local execution mode the Simulation Probability 
#       Tables, which contain observed probabilities relationships between actual 
#       conditions and drugs, can be generated in the same run as the Simulated Persons.
#       In a distributed mode run, the Simulation Probability Tables are generated 
#       first, in an initial execution.  These Simulation Probability Tables are used as
#       input to multiple, distributed executions of the OSIM 2 simulation to generate 
#       Simulated Persons.  Since the distributed Simulated Person Data were generated 
#       using the same input Tables, the files can be concatenated into one larger 
#       Simulated Person file at the conclusion of the distributed runs.  
#
#       To allow the simulated dataset to approximate characteristics of real 
#       observational data, the analysis phase of the package performs a preliminary analysis
#       of an actual claims database (in CDM format).  Descriptive statistics are calculated 
#       to estimate the categories and probability distributions parameters for:
#           � Gender
#           � Age (Year of Birth)
#           � Number of Distinct Conditions
#           � Observation period duration
#           � Condition to Condition (first occurrence) Transition Probability
#           � Number of Condition Eras for a Condition Probability
#           � Number of Distinct Drugs Probability
#           � Number of Drug Chances for a Condition Probability
#           � Condition (any occurrence) to Drug (first occurrence) Transition Probability
#           � Number of Drug Eras for a Drug Probability
#           � Total Drug Exposure Duration for all Drug Eras for a Drug Probability
#
#
#   These probability distributions, which are described in detail in the Data 
#   Dictionary for the Observational Medical Dataset Simulator, are input to 
#   the OSIM 5 simulation and used to generate the Simulated Persons.
#
#*******************************************************************************
#
#   Contents of this #ReadMe.txt file
#
#    1. System Requirements
#    2. Standard Synonyms
#    3. Standard Views
#    4. User Modifiable Range Functions
#    5. Transition Probabilty Tables
#    6. Other Tables
#    7. process_log Table
#    8. Module 1: osim2.analyze_source_db()
#    9. OSIM 2: Simulated Data
#   10. Module 2: osim2.ins_sim_data(person_count,person_start_id)
#   11. Module 3a: osim2.ins_outcomes()
#   12. Module 3: osim2.copy_final_data(destination_schema,total_person_count)
#   13. Other OSIM 2 Methods
#   14. Standard Package Creation Example
#   15. Simulation Scenarios
#
################################################################################

1. System Requirements

    Oracle 11g - the package has not been tested with earlier versions of Oracle
    Oracle SQL*Plus

################################################################################

2. Standard Synonyms

    In order to analyze a CDM format database, OSIM 2 uses a set of standard 
    synonyms.  This allows the package to be run against any CDM database without
    renaming the tables or modifying the OSIM 2 package.  The synonyms must be 
    created for the person, observation_period, conditon_era, and drug_era tables
    in order for the package to successfully compile and run.
    ============================================================================
    Example SQL*Plus script for synonym creation:
    CREATE OR REPLACE SYNONYM s_person FOR mslr_cdm.person;
    /
    CREATE OR REPLACE SYNONYM s_condition_era FOR mslr_cdm.condition_era;
    /
    CREATE OR REPLACE SYNONYM s_observation_period FOR mslr_cdm.observation_period;
    /
    CREATE OR REPLACE SYNONYM s_drug_era FOR mslr_cdm.drug_era;
    /
    COMMIT;
    EXIT;

################################################################################

3. Standard Views

  OSIM 2 accesses the synonymed CDM tables through a standard set of read-only views.  
  The views are separate from the OSIM 2 package and can be slightly modified with
  specialized filtering to limit the analysis (ex. gender_concept_id, person_id range).
  The views must be created after the synonyms, and like the synonyms, are necessary
  for compile and execution of the OSIM 2 package.  The standard views are contained
  in OSIM2_views.sql and can be executed in SQL*Plus.
  
  If the persistence window needs to be changed for drug or condition eras, the 
  selected condition_occurrence_type and drug_exposure_type must be modified in these
  views.
  
  v_src_person -- This view selects the persons to analyze.  The standard view limits
    selection to persons with an observation_period record and year_of_birth value.

  v_src_person_strata -- This view returns the persons in the v_src_person view with
    a few additional precalculated values commonly used by the analysis, including
    distinct condition count, distinct drug count, and age.
    
  v_observation_period -- This view returns the observation_period rows for the
    persons in the v_src_person view.
    
  v_src_condition_era1_ids -- This view returns all IDs of the condition_eras.
  
  v_src_condition_era1 -- This view returns all condition_era rows.
  
  v_src_first_conditions -- This view returns only the first occurrence condition
    eras.
    
  v_all_conditions -- This view returns all condition_eras including a precalculated
    person age at conditon start value.
    
  v_src_drug_era1 -- This view returns all drug_eras.
  
  v_src_first_drugs -- This view returns only the first occurrence drug_eras.

################################################################################
  
4. User Modifiable Range Functions

  The user-modifiable range functions are used by both the database analysis and 
  simulation phases of OSIM 2.  They specify the ranges of various strata to be 
  bucketed together for the transition probabilities. The user has control over 
  the categories and related ranges associated with some of the key distributions, 
  via a set of functions in the OSIM 2 Oracle package.  The default buckets were
  derived from trial and error during OSIM 2 development.  The functions are 
  described in more detail in the Data dictionary and Process Design 
  documentation.
  
  The simulation must run with the same range functions that were used to create 
  the transition tables.
  
  This modification is required to the OSIM 2 package itself, and the changes 
  should be saved accordingly.
  
################################################################################    

5. Transition Probabilty Tables

  The OSIM 2 Analysis uses a set of 13 tables to store probability
  characteristics of the analyzed CDM database.
  
  These tables must be created in order to compile and run the OSIM 2 package.
  
  The SQL*Plus script to create the tables is in OSIM2_tables.sql.
  
  The tables are described in detail in the Data Dictionary and Process Design 
  documentation.
  
################################################################################    

6. Other Tables

  The OSIM 2 package uses a set of standard tables to generate simulated data.
  
  These tables must be created in order to compile and run the OSIM 2 package.
  
  The SQL*Plus script to create the tables is also contained in 
  OSIM2_tables.sql.
  
  The tables are described in more detail in the Data Dictionary and Process Design 
  documentation.

################################################################################    

7. process_log Table

  The OSIM 2 package performs extensive logging during analysis and simulation.
  As messages are posted to the process_log they are individually committed, so 
  the table can be monitored from another session to check the progress of the
  current OSIM 2 process.
  
  The SQL*Plus script to create the tables is also contained in 
  OSIM2_tables.sql.
  
################################################################################    

8. Module 1: osim2.analyze_source_db()

  This OSIM 2 package method performs all the CDM database analysis.  It has
  no additonal parameters.
  
  The method will truncate and repopulate all the Transition Probability Tables.
  
  This method may take several days to run, depending on the size of the CDM 
  database being analyzed.  Its progress can be monitored in the process_log 
  table.
  
################################################################################ 

9. OSIM 2: Simulated Data

   The simulated data will be inserted into four CDM format tables:
   osim_person
   osim_observation_period
   osim_condition_era
   osim_drug_era
   
  For a non-parallel run, these tables will contain the final simulated data.
  The tables can be renamed or copied to standard CDM tables using the 
  osim2.copy_final_data() method.
  
  For a parallel run, the condition era and drug era IDs will not be unique, and 
  will require renumbering by the osim2.copy_final_data() method.
   
  The SQL*Plus script to create these tables is also contained in 
  OSIM2_tables.sql.

################################################################################ 
  
10. Module 2: osim2.ins_sim_data(person_count,person_start_id)

   Optional parameters

     person_count -- number of persons to simulate (default=5000)
     
     person_start_id -- the starting person_id to use (default=next incremental value)

   This OSIM 2 method simulates presons, observation periods, condition eras,
   and drug eras using the Transition Probabilty Tables.  It does not directly access
   the original CDM data.
  
   The method can be run multiple times in succession to append more and more data 
   to the "osim_" prefixed CDM tables.
   
   The method can be run in parallel with different person_start_id values so that
   each run generates a unique range of person_id values.
   
################################################################################ 
  
11. Module 3a: osim2.ins_outcomes()

   Optional Outcomes
   
   Increased risk in harmful outcomes following drug exposure could be indicative of 
   potential drug adverse reactions that warrant further consideration.  The 
   outcome simulation procedure will incorporate the relationship between drugs and 
   outcomes by introducing additional cases of the condition into the sample based 
   on the attributable risk of the condition due to the drug. 
   
   The format of the input outcomes table is described in more detail in the
   Data Dictionary and Process Design documentation.
   
   The osim_drug_outcome table can be manually populated with known condition / drug 
   outcome effects.  Running the optional outcome module will apply the rules, and 
   either remove or insert condition eras to obtain the specified relative risk.

   This method only runs against the "osim_" prefixed tables and must be run before 
   the final copy method.
    
################################################################################ 

12. Module 3: osim2.copy_final_data(destination_schema,total_person_count)

   Optional Parameters

     destination_schema -- destination schema containg standard CDM tables (person,
       observation_period, drug_era, and condition_era)  (default=current schema)
     
     total_person_count -- the exact number of persons that must exist in the 
       osim_person table in order to perform the copy.  This parameter allows each
       parallel execution to attempt the final copy.
     
   Final Copy
   
    This method is only required for parallel data finalization (for drug and
    condition era renumbering), though it can also be used to final copy non-parallel 
    simulated data to standard-named CDM tables in any schema.

################################################################################ 
    
13. Other OSIM 2 Methods

     osim2.delete_all_sim_data() -- will delete all data from "osim_" prefixed tables
     osim2.drop_osim_indexes() -- will drop all indexes from "osim_" prefixed tables
     osim2.create_osim_indexes() -- will create all indexes from "osim_" prefixed tables

################################################################################ 

14. Standard Package Creation Example

   The schema must have explicit (not just through role) select permissions for
   the CDM tables.

   After saving synonym creates in a script file:
   
   >>sqlplus schema/password@dbserver @synonyms.sql
   >>sqlplus schema/password@dbserver @OSIM2_views.sql
   >>sqlplus schema/password@dbserver @OSIM2_package.sql
   
################################################################################ 

15. Simulation Scenarios

   All command blocks should be executed inside SQL*Plus

   --========================================================================
   --Simple non-parallel analysis and simulation of 100,000 persons
   begin
     osim2.analyze_source_db();
     osim2.ins_sim_data(100000);
   end;
   /
   
   --========================================================================
   --Simple non-parallel analysis and simulation of 100,000 persons
   --with final copy to another schema
   begin
     osim2.analyze_source_db();
     osim2.ins_sim_data(100000);
     osim2.copy_final_data('final_schema',100000);
   end;
   /
       
   --========================================================================
   --Simple non-parallel analysis and simulation of 100,000 persons
   --with outcomes and final copy to another schema
   begin
     osim2.analyze_source_db();
     osim2.ins_sim_data(100000);
     osim2.ins_outcomes();
     osim2.copy_final_data('final_schema',100000);
   end;
   /
   
   --========================================================================
   -- Analysis and parallel simulation of 1,000,000 persons (4 x 250,000)
   -- ANALYSIS MUST COMPLETE BEFORE STARTING SIMULATIONS
   begin
     osim2.analyze_source_db();
   end;
   /

   --Parallel Simulation 1
   begin
     osim2.ins_sim_data(250000,1);
     osim2.copy_final_data('final_schema',1000000);
   end;
   /

   --Parallel Simulation 2
   begin
     osim2.ins_sim_data(250000,250001);
     osim2.copy_final_data('final_schema',1000000);
   end;
   /
   
   --Parallel Simulation 3
   begin
     osim2.ins_sim_data(250000,500001);
     osim2.copy_final_data('final_schema',1000000);
   end;
   /

   --Parallel Simulation 4
   begin
     osim2.ins_sim_data(250000,750001);
     osim2.copy_final_data('final_schema',1000000);
   end;
   /

################################################################################ 
