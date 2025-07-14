/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
USE HealthClaims;
GO

IF OBJECT_ID('silver.emr_hospital_a_patients', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_a_patients;
GO

CREATE TABLE silver.emr_hospital_a_patients (
    pat_id              NVARCHAR(50),
    pat_first_name       NVARCHAR(50),
    pat_last_name        NVARCHAR(50),
    pat_middle_name      NVARCHAR(50),
    pat_ssn             NVARCHAR(50),
    pat_phonenumber     NVARCHAR(50),
    pat_gender          NVARCHAR(50),
    pat_dob             DATE,
    pat_age             int,
    pat_address         NVARCHAR(100),
    pat_src_modified_dt   DATE,
    pat_insert_dt   DATE
);
GO

IF OBJECT_ID('silver.emr_hospital_a_departments', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_a_departments;
GO

CREATE TABLE silver.emr_hospital_a_departments (
    dept_id       NVARCHAR(50),
    dept_name     NVARCHAR(50),
    dept_insert_dt  DATE
);
GO

IF OBJECT_ID('silver.emr_hospital_a_encounters', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_a_encounters;
GO

CREATE TABLE silver.emr_hospital_a_encounters (
    enctr_id            NVARCHAR(50),
    enctr_pat_id        NVARCHAR(50),
    enctr_dt            DATE,
    enctr_type          NVARCHAR(50),
    enctr_provider_id   NVARCHAR(50),
    enctr_dept_id       NVARCHAR(50),
    enctr_procedure_code NVARCHAR(50),
    enctr_src_insert_dt    DATE,
    enctr_src_modified_dt  DATE,
    enctr_insert_dt    DATE
);
GO

IF OBJECT_ID('silver.emr_hospital_a_providers', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_a_providers;
GO

CREATE TABLE silver.emr_hospital_a_providers (
    provider_id          NVARCHAR(50),
    provider_firstname   NVARCHAR(50),
    provider_lastname    NVARCHAR(50),
    provider_specialization    NVARCHAR(50),
    provider_dept_id       NVARCHAR(50),
    provider_npi           NVARCHAR(50),
    provider_insert_dt      DATE
    );
GO

IF OBJECT_ID('silver.emr_hospital_a_transaction', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_a_transaction;
GO

CREATE TABLE silver.emr_hospital_a_transaction (
    trans_id             NVARCHAR(50),
    trans_enctr_id       NVARCHAR(50),
    trans_pat_id         NVARCHAR(50),
    trans_provider_id    NVARCHAR(50),
    trans_dept_id        NVARCHAR(50),
    trans_visit_dt       DATE,
    trans_service_dt     DATE,
    trans_paid_id        DATE,
    trans_visit_type     NVARCHAR(50),
    trans_amount         DECIMAL(10,2),
    trans_amount_type    NVARCHAR(50),
    trans_paid_amount    DECIMAL(10,2),
    trans_claim_id       NVARCHAR(50),
    trans_payer_id       NVARCHAR(50),
    trans_procedure_code NVARCHAR(50),
    trans_icd_code       NVARCHAR(50),
    trans_lob            NVARCHAR(50),
    trans_medicaid_id    NVARCHAR(50),
    trans_medicare_id    NVARCHAR(50),
    trans_src_insert_dt      DATE,
    trans_src_modified_dt    DATE,
    trans_insert_dt      DATE
);
GO
IF OBJECT_ID('silver.emr_hospital_b_patients', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_b_patients;
GO

CREATE TABLE silver.emr_hospital_b_patients (
    pat_id              NVARCHAR(50),
    pat_first_name       NVARCHAR(50),
    pat_last_name        NVARCHAR(50),
    pat_middle_name      NVARCHAR(50),
    pat_ssn             NVARCHAR(50),
    pat_phonenumber     NVARCHAR(50),
    pat_gender          NVARCHAR(50),
    pat_dob             DATE,
    pat_age             int,
    pat_address         NVARCHAR(100),
    pat_src_modified_dt   DATE,
    pat_insert_dt       DATE
);
GO

IF OBJECT_ID('silver.emr_hospital_b_departments', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_b_departments;
GO

CREATE TABLE silver.emr_hospital_b_departments (
    dept_id       NVARCHAR(50),
    dept_name     NVARCHAR(50),
    dept_insert_dt DATE
);
GO

IF OBJECT_ID('silver.emr_hospital_b_encounters', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_b_encounters;
GO

CREATE TABLE silver.emr_hospital_b_encounters (
    enctr_id            NVARCHAR(50),
    enctr_pat_id        NVARCHAR(50),
    enctr_dt            DATE,
    enctr_type          NVARCHAR(50),
    enctr_provider_id   NVARCHAR(50),
    enctr_dept_id       NVARCHAR(50),
    enctr_procedure_code NVARCHAR(50),
    enctr_src_insert_dt    DATE,
    enctr_src_modified_dt  DATE,
    enctr_insert_dt DATE
);
GO

IF OBJECT_ID('silver.emr_hospital_b_providers', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_b_providers;
GO

CREATE TABLE silver.emr_hospital_b_providers (
    provider_id    NVARCHAR(50),
    provider_firstname   NVARCHAR(50),
    provider_lastname    NVARCHAR(50),
    provider_specialization    NVARCHAR(50),
    provider_dept_id       NVARCHAR(50),
    provider_npi    NVARCHAR(50),
    provider_insert_dt DATE

    );
GO

IF OBJECT_ID('silver.emr_hospital_b_transaction', 'U') IS NOT NULL
    DROP TABLE silver.emr_hospital_b_transaction;
GO

CREATE TABLE silver.emr_hospital_b_transaction (
    trans_id             NVARCHAR(50),
    trans_enctr_id       NVARCHAR(50),
    trans_pat_id         NVARCHAR(50),
    trans_provider_id    NVARCHAR(50),
    trans_dept_id        NVARCHAR(50),
    trans_visit_dt       DATE,
    trans_service_dt     DATE,
    trans_paid_id        DATE,
    trans_visit_type     NVARCHAR(50),
    trans_amount         DECIMAL(10,2),
    trans_amount_type    NVARCHAR(50),
    trans_paid_amount    DECIMAL(10,2),
    trans_claim_id       NVARCHAR(50),
    trans_payer_id       NVARCHAR(50),
    trans_procedure_code NVARCHAR(50),
    trans_icd_code       NVARCHAR(50),
    trans_lob            NVARCHAR(50),
    trans_medicaid_id    NVARCHAR(50),
    trans_medicare_id    NVARCHAR(50),
    trans_src_insert_dt      DATE,
    trans_src_modified_dt    DATE,
    trans_insert_dt      DATE
);

IF OBJECT_ID('silver.cpt_codes', 'U') IS NOT NULL
    DROP TABLE silver.cpt_codes;
GO

CREATE TABLE silver.cpt_codes (
    cpt_proc_cat       NVARCHAR(50),
    cpt_codes          NVARCHAR(50),
    cpt_proc_desc      NVARCHAR(1000),
    cpt_code_status    NVARCHAR(50),
    cpt_insert_dt      DATE
);
GO

IF OBJECT_ID('silver.claims_hosp_a', 'U') IS NOT NULL
    DROP TABLE silver.claims_hosp_a;
GO

CREATE TABLE silver.claims_hosp_a (
    clm_id          NVARCHAR(50),
    clm_trans_id    NVARCHAR(50),
    clm_pat_id      NVARCHAR(50),
    clm_enctr_id    NVARCHAR(50),
    clm_provider_id NVARCHAR(50),
    clm_dept_id     NVARCHAR(50),
    clm_service_dt  DATE,
    clm_dt          DATE,
    clm_payer_id    NVARCHAR(50),
    clm_amt         DECIMAL(10,2),
    clm_paid_amt    DECIMAL(10,2),
    clm_status      NVARCHAR(50),
    clm_payer_typ   NVARCHAR(50),
    clm_deductible   DECIMAL(10,2),
    clm_coinsurance  DECIMAL(10,2),
    clm_copay        DECIMAL(10,2),
    clm_sr_insert_dt    DATE,
    clm_src_mod_id       DATE,
    clm_insert_dt       DATE

);
GO

IF OBJECT_ID('silver.claims_hosp_b', 'U') IS NOT NULL
    DROP TABLE silver.claims_hosp_b;
GO

CREATE TABLE silver.claims_hosp_b (
    clm_id          NVARCHAR(50),
    clm_trans_id    NVARCHAR(50),
    clm_pat_id      NVARCHAR(50),
    clm_enctr_id    NVARCHAR(50),
    clm_provider_id NVARCHAR(50),
    clm_dept_id     NVARCHAR(50),
    clm_service_dt  DATE,
    clm_dt          DATE,
    clm_payer_id    NVARCHAR(50),
    clm_amt         DECIMAL(10,2),
    clm_paid_amt    DECIMAL(10,2),
    clm_status      NVARCHAR(50),
    clm_payer_typ   NVARCHAR(50),
    clm_deductible   DECIMAL(10,2),
    clm_coinsurance  DECIMAL(10,2),
    clm_copay        DECIMAL(10,2),
    clm_src_insert_dt    DATE,
    clm_src_mod_id       DATE,
    clm_insert_dt       DATE
);
GO