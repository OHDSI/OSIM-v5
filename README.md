# OSIM_v5 version 1.0.001
OMOP CDM v5 version for OSIM simulator - Observational Medical Outcomes Partnership

## Initial Version
Oracle PL/SQL: Rich Murray, United BioSource Corporation <br/>
Last modified: 15 February 2011 <br/>
2010 Foundation for the National Institutes of Health <br/></br>
IMPORTANT NOTE: 
<br/>
Most of this documentation and code logic is identical to version 2, with syntactical changes as 
required for the new format and PsotgreSQL conversion. 
<br/>
Please refer to the documentation available in v2 Documentation folder, or the paper avaibale at: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3243118/ for more information.

## Current Version
Written in PostgreSQL: Kausar Mukadam, Georgia Tech Research Institute


##   Description

OSIM 5 is a OMOP CDM v5 compatible procedure for constructing simulated observational datasets.  The simulated datasets are modeled after real observational data sources, but consist of synthetic persons 
with simulated drug exposures and condition occurrences. These condition/ drug instances are based on random draws from probability distributions. 
These distribution are modeled after the relationships betwwen real like drugs and conditions. 
<br/><br/>
This package was built on PostgreSQL. 


## Execution

### Step 1: Required Views

    In order to analyze a CDM format database, the schema and tables for the source data need to be specified in OSIM 5.
    Modify the first 4 views in OSIM5_views.sql to point to the required tables. The final views should look as follows
    
    ============================================================================
    Example view creation:
    CREATE OR REPLACE VIEW s_person as select * from synpuf5.person;
    CREATE OR REPLACE VIEW s_condition_era as select * from synpuf5.condition_era;
    CREATE OR REPLACE VIEW s_observation_period as select * from synpuf5.observation_period;
    CREATE OR REPLACE VIEW s_drug_era as select * from synpuf5.drug_era;



### Step 2: Standard Views

  The above generated views are accessed through a standard set of read-only views. These are contained in the OSIM5_views.sql 
  file and are seperate from the OSIM package and can be slightly modified with
  specialized filtering to limit the analysis (ex. gender_concept_id, person_id range). 
  
  
  After modifying the initial views in step 1, these additional views must be created by executing the OSIM5_views.sql script
  in Postgres. 
  
  
  NOTE: Change from version 2! - The persistance window cannot be changed in this version of the package. The code uses 
  the standard persistance windows and does not filter based on persistance.
  
  
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

  
### Step 3 (Optional): User Modifiable Range Functions

  The user-modifiable range functions are used by both the database analysis and 
  simulation phases of OSIM.  They specify the bucketing of transition probabilities. 
  The user can control these ranges and bucketing by modifying the function in OSIM5_package.sql. The default buckets were
  are identicla to version 2 (which were derived from trial and error during development).  The functions are 
  described in more detail in the Data dictionary and Process Design 
  documentation of OSIM 2 avaiable in v2 documentation folder.
  
  Please note: The same range functions must be used during anaysis and simulation phases.
  

### Step 4: Table Generation
The OSIM 5 package uses some tables (in the anaysis stage, to store final synthetic data, etc), whcih need to be created before 
the package is executed. This can be done by executing the OSIM5_tables.sql file. The tables are described in detail in the Data Dictionary and Process Design documentation of OSIM 2.

### Step 5: Analysis Phase: analyze_source_db()

OSIM 5 is based on transition probabilty tables which are used to store probability characteristics of the 
source CDM database. The OSIM5 package method analyze_source_db() performs all the CDM database analysis.  


This method will truncate and repopulate all the Transition Probability Tables.
  
  
NOTE: This part of the process is time and computationlly intensive and may require several days to run, 
depending on the size of the database being analyzed.  In version 2 the progress can be monitored in the process_log 
table, but since Postgres does not have support for autonomous transactions, the progress can be monitored through DEBUG statements
by setting the postgres log level to DEBUG.
  
### Step 6: Simulated Data: ins_sim_data(person_count,person_start_id)

The simulated data will be inserted into four CDM format tables: <br>
   osim_person <br>
   osim_observation_period <br>
   osim_condition_era <br>
   osim_drug_era 
   
 Patients are simulated through the ins_sim_data() method of the OSIM 5 package. 
 
 
 Optional parameters
  <br/>
     person_count -- number of persons to simulate (default=5000)
  <br/>
     person_start_id -- the starting person_id to use (default=next incremental value)


   The method can be run multiple times in succession to append more and more data 
   to the "osim_" prefixed CDM tables.
   
### Step 7(Optional): Outcomes

In Progress
   
### Step 8(Optional): Copy Data
In Progress

## Other OSIM Methods

    delete_all_sim_data() -- will delete all data from "osim_" prefixed tables
    
    
    drop_osim_indexes() -- will drop all indexes from "osim_" prefixed tables
    
    
    create_osim_indexes() -- will create all indexes from "osim_" prefixed tables


## Simulation Scenarios

   All command blocks should be executed inside PostgreSQL


   ========================================================================<br/>
   -- Simple non-parallel analysis and simulation of 100,000 persons <br/>
   begin <br/>
     analyze_source_db(); <br/>
     ins_sim_data(100000); <br/>
   end; <br/>
   
   
   ========================================================================<br/>
   -- Analysis and parallel simulation of 50,000 persons (2 x 250,000) <br/>
   -- ANALYSIS MUST COMPLETE BEFORE STARTING SIMULATIONS <br/>
   begin <br/>
     analyze_source_db(); <br/>
   end; <br/>
   /


   --Parallel Simulation 1 <br/>
   begin <br/>
     ins_sim_data(250000,1); <br/>
   end; <br/>
   /


   --Parallel Simulation 2 <br/>
   begin <br/>
     ins_sim_data(250000,250001); <br/>
   end; <br/> 
   /
