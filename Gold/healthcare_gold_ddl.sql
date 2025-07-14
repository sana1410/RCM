/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_patients
-- =============================================================================
IF OBJECT_ID('gold.dim_patients', 'V') IS NOT NULL
    DROP VIEW gold.dim_patients;
GO

CREATE VIEW gold.dim_patients AS
SELECT * from silver.emr_hospital_a_patients
Union ALL
Select * from silver.emr_hospital_b_patients;  
GO



-- =============================================================================
-- Create Dimension: gold.dim_departments
-- =============================================================================
IF OBJECT_ID('gold.dim_departments', 'V') IS NOT NULL
    DROP VIEW gold.dim_departments;
GO

CREATE VIEW gold.dim_departments AS
SELECT * from silver.emr_hospital_a_departments
Union ALL
Select * from silver.emr_hospital_b_departments;    
GO

-- =============================================================================
-- Create Fact Table: gold.fact_transactions
-- =============================================================================
IF OBJECT_ID('gold.fact_transactions', 'V') IS NOT NULL
    DROP VIEW gold.fact_transactions;
GO

CREATE VIEW gold.fact_transactions AS
SELECT * from silver.[emr_hospital_a_transaction]
Union ALL
Select * from silver.[emr_hospital_b_transaction];    
GO

-- =============================================================================
-- Create Fact Table: gold.dim_providers
-- =============================================================================
IF OBJECT_ID('gold.dim_providers', 'V') IS NOT NULL
    DROP VIEW gold.dim_providers;
GO

CREATE VIEW gold.dim_providers AS
SELECT * from silver.[emr_hospital_a_providers]
Union ALL
Select *  from silver.[emr_hospital_b_providers];    
GO

-- =============================================================================
-- Create Fact Table: gold.fact_claims
-- =============================================================================
IF OBJECT_ID('gold.fact_claims', 'V') IS NOT NULL
    DROP VIEW gold.fact_claims;
GO

CREATE VIEW gold.fact_claims AS
SELECT * from silver.[claims_hosp_a]
Union ALL
Select *  from silver.[claims_hosp_b];    
GO

-- =============================================================================
-- Create Dimension: gold.dim_encounters
-- =============================================================================
IF OBJECT_ID('gold.dim_encounters', 'V') IS NOT NULL
    DROP VIEW gold.dim_encounters;
GO

CREATE VIEW gold.dim_encounters AS
SELECT *  from silver.[emr_hospital_a_encounters]
Union ALL
Select * from silver.[emr_hospital_b_encounters];    
GO