--================================================================================
-- ORACLE PACKAGE OSIM2 v1.5.001
-- Person/Condition Simulator
--
-- Oracle SQL for inserting standard OMOP HOI outcomes
-- 
--================================================================================
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
--================================================================================
--Oracle SQL Script for inserting OMOP HOI Outcomes into OSIM 2 Drug Outcome Table
SET SEARCH_PATH TO synthetic_data_generation;
INSERT INTO osim_drug_outcome
 (risk_or_benefit, drug_concept_id, condition_concept_id, relative_risk,
 outcome_risk_type, outcome_onset_days_min, outcome_onset_days_max)
SELECT 'risk', 600000001, 500000101, 0.03, 'first exposure', 0, 30
UNION
SELECT 'risk', 600000001, 500000102, 0.03, 'first exposure', 0, 30 
UNION
SELECT 'risk', 600000004, 500000201, 0.025, 'first exposure', 15, 90 
UNION
SELECT 'risk', 600000004, 500000202, 0.025, 'first exposure', 15, 90  
UNION
SELECT 'risk', 600000004, 500000203, 0.025, 'first exposure', 15, 90  
UNION
SELECT 'risk', 600000004, 500000204, 0.025, 'first exposure', 15, 90  
UNION
SELECT 'risk', 600000004, 500000205, 0.025, 'first exposure', 15, 90  
UNION
SELECT 'risk', 600000003, 500000301, 0.013, 'any exposure', 1, 45  
UNION
SELECT 'risk', 600000003, 500000302, 0.013, 'any exposure', 1, 45  
UNION
SELECT 'risk', 600000003, 500000303, 0.013, 'any exposure', 1, 45  
UNION
SELECT 'risk', 600000003, 500000304, 0.013, 'any exposure', 1, 45  
UNION
SELECT 'risk', 600000003, 500000305, 0.013, 'any exposure', 1, 45  
UNION
SELECT 'risk', 600000003, 500000306, 0.013, 'any exposure', 1, 45  
UNION
SELECT 'risk', 600000003, 500000307, 0.013, 'any exposure', 1, 45  
UNION
SELECT 'risk', 600000002, 500000401, 0.05, 'insidious', 0, NULL   
UNION
SELECT 'risk', 600000002, 500000402, 0.05, 'insidious', 0, NULL  
UNION
SELECT 'risk', 600000002, 500000403, 0.05, 'insidious', 0, NULL  
UNION
SELECT 'risk', 600000002, 500000404, 0.05, 'insidious', 0, NULL  
UNION
SELECT 'risk', 600000010, 500000501, 0.02, 'insidious', 0, NULL  
UNION
SELECT 'risk', 600000010, 500000502, 0.02, 'insidious', 0, NULL  
UNION
SELECT 'risk', 600000010, 500000503, 0.02, 'insidious', 0, NULL  
UNION
SELECT 'risk', 600000005, 500000601, 0.018, 'accumulative', 3, NULL  
UNION
SELECT 'risk', 600000005, 500000602, 0.018, 'accumulative', 3, NULL  
UNION
SELECT 'risk', 600000005, 500000603, 0.018, 'accumulative', 3, NULL  
UNION
SELECT 'risk', 600000005, 500000604, 0.018, 'accumulative', 3, NULL  
UNION
SELECT 'risk', 600000008, 500000801, 0.012, 'accumulative', 60, NULL  
UNION
SELECT 'risk', 600000009, 500000801, 0.015, 'insidious', 1, NULL  
UNION
SELECT 'risk', 600000008, 500000802, 0.012, 'accumulative', 60, NULL  
UNION
SELECT 'risk', 600000009, 500000802, 0.015, 'insidious', 1, NULL  
UNION
SELECT 'risk', 600000008, 500000803, 0.012, 'accumulative', 60, NULL  
UNION
SELECT 'risk', 600000009, 500000803, 0.015, 'insidious', 1, NULL  
UNION
SELECT 'risk', 600000008, 500000804, 0.012, 'accumulative', 60, NULL  
UNION
SELECT 'risk', 600000009, 500000804, 0.015, 'insidious', 1, NULL  
UNION
SELECT 'risk', 600000007, 500001001, 0.0125, 'insidious', 1, NULL  
UNION
SELECT 'risk', 600000007, 500001002, 0.0125, 'insidious', 1, NULL  
UNION
SELECT 'benefit', 600000001, 500000701, 0.008, 'accumulative', 1, NULL  
UNION
SELECT 'benefit', 600000006, 500000901, 0.0075, 'first exposure', 1, 180  
UNION
SELECT 'benefit', 600000006, 500000902, 0.0075, 'first exposure', 1, 180  
UNION
SELECT 'benefit', 600000006, 500000903, 0.0075, 'first exposure', 1, 180  
UNION
SELECT 'benefit', 600000006, 500000904, 0.0075, 'first exposure', 1, 180  ;
