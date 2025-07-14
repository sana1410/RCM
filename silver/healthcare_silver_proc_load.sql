/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
use HealthClaims;
GO
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Patients Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.emr_hospital_a_patients
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.emr_hospital_a_patients';
		TRUNCATE TABLE silver.emr_hospital_a_patients;
		PRINT '>> Inserting Data Into: silver.emr_hospital_a_patients';
		INSERT INTO silver.emr_hospital_a_patients (
		                                            pat_id,
                                                    pat_first_name ,
                                                    pat_last_name   ,
                                                    pat_middle_name ,
                                                    pat_ssn         ,
                                                    pat_phonenumber ,
                                                    pat_gender      ,
                                                    pat_dob         ,
                                                    pat_age,
                                                    pat_address     ,
                                                    pat_src_modified_dt ,
                                                    pat_insert_dt
         )
        Select 
        pat_id          ,
        pat_first_name  ,
        pat_last_name   ,
        pat_middle_name ,
        pat_ssn         ,
        pat_phonenumber ,
        pat_gender      ,
        pat_dob ,
        DATEDIFF(YEAR,pat_dob,CAST(GETDATE() AS DATE)) as pat_age,
        pat_address,
        CASE
            WHEN pat_dob > pat_modified_dt  THEN  CAST(GETDATE() AS DATE)
            ELSE pat_modified_dt 
        END as pat_src_modified_dt,
        CAST(GETDATE() AS DATE) as pat_insert_dt
        from bronze.emr_hospital_a_patients;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.emr_hospital_b_patients
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.emr_hospital_b_patients';
		TRUNCATE TABLE silver.emr_hospital_b_patients;
		PRINT '>> Inserting Data Into: silver.emr_hospital_b_patients';
		INSERT INTO silver.emr_hospital_b_patients (
		                                            pat_id,
                                                    pat_first_name ,
                                                    pat_last_name   ,
                                                    pat_middle_name ,
                                                    pat_ssn         ,
                                                    pat_phonenumber ,
                                                    pat_gender      ,
                                                    pat_dob         ,
                                                    pat_age         ,
                                                    pat_address     ,
                                                    pat_src_modified_dt ,
                                                    pat_insert_dt
         )
        Select 
        concat('HOSP2-',substring(pat_id,7,len(pat_id))) as pat_id           ,
        pat_first_name  ,
        pat_last_name   ,
        pat_middle_name ,
        pat_ssn         ,
        pat_phonenumber ,
        pat_gender      ,
        pat_dob ,
        DATEDIFF(YEAR,pat_dob,CAST(GETDATE() AS DATE)) as pat_age,
        pat_address,
        CASE
            WHEN pat_dob > pat_modified_dt  THEN  CAST(GETDATE() AS DATE)
            ELSE pat_modified_dt 
        END as pat_src_modified_dt,
        CAST(GETDATE() AS DATE) as pat_insert_dt
        from bronze.emr_hospital_b_patients;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading Departments Tables';
		PRINT '------------------------------------------------';
        -- Loading silver.emr_hospital_a_departments

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.emr_hospital_a_departments';
		TRUNCATE TABLE silver.emr_hospital_a_departments;
		PRINT '>> Inserting Data Into: silver.emr_hospital_a_departments';
		INSERT INTO silver.emr_hospital_a_departments (
                                                        dept_id,
                                                        dept_name,
                                                        dept_insert_dt
        )
        select 
        concat('HOSP1-',dept_id) as dept_id ,
        dept_name,
        CAST(GETDATE() AS DATE) as dept_insert_dt
        from bronze.emr_hospital_a_departments;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.emr_hospital_b_departments
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.emr_hospital_b_departments';
		TRUNCATE TABLE silver.emr_hospital_b_departments;
		PRINT '>> Inserting Data Into: silver.emr_hospital_b_departments';
		INSERT INTO silver.emr_hospital_b_departments (
                                                        dept_id,
                                                        dept_name,
                                                        dept_insert_dt
        )
        select 
        concat('HOSP2-',dept_id) as dept_id ,
        dept_name,
        CAST(GETDATE() AS DATE) as dept_insert_dt
        from bronze.emr_hospital_b_departments;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading Claims Tables';
		PRINT '------------------------------------------------';
        -- Loading silver.claims_hosp_a

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.claims_hosp_a';
		TRUNCATE TABLE silver.claims_hosp_a;
		PRINT '>> Inserting Data Into: silver.claims_hosp_a';
		INSERT INTO silver.claims_hosp_a (
                                           [clm_id]
                                          ,[clm_trans_id]
                                          ,[clm_pat_id]
                                          ,[clm_enctr_id]
                                          ,[clm_provider_id]
                                          ,[clm_dept_id]
                                          ,[clm_service_dt]
                                          ,[clm_dt]
                                          ,[clm_payer_id]
                                          ,[clm_amt]
                                          ,[clm_paid_amt]
                                          ,[clm_status]
                                          ,[clm_payer_typ]
                                          ,[clm_deductible]
                                          ,[clm_coinsurance]
                                          ,[clm_copay]
                                          ,[clm_sr_insert_dt]
                                          ,[clm_src_mod_id]
                                          ,[clm_insert_dt]
        )
        select 
         concat('HOSP1-',clm_id) as clm_id
        ,concat('HOSP1-',clm_trans_id) as clm_trans_id
        ,clm_pat_id
        ,concat('HOSP1-',clm_enctr_id) as clm_enctr_id
        ,concat('H1-',clm_provider_id) as clm_provider_id
        ,concat('HOSP1-',clm_dept_id) as clm_dept_id
        ,clm_service_dt
        ,clm_dt
        ,concat('HOSP1-',clm_payer_id) as clm_payer_id 
        ,clm_amt
        ,clm_paid_amt
        ,clm_status
        ,clm_payer_typ
        ,clm_deductible
        ,clm_coinsurance
        ,clm_copay
        ,clm_insert_dt
        ,clm_mod_id
        ,CAST(GETDATE() AS DATE)
        from 
        [HealthClaims].[bronze].[claims_hosp_a]
        
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

       -- Loading silver.claims_hosp_b

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.claims_hosp_b';
		TRUNCATE TABLE silver.claims_hosp_b;
		PRINT '>> Inserting Data Into: silver.claims_hosp_b';
		INSERT INTO silver.claims_hosp_b (
                                           [clm_id]
                                          ,[clm_trans_id]
                                          ,[clm_pat_id]
                                          ,[clm_enctr_id]
                                          ,[clm_provider_id]
                                          ,[clm_dept_id]
                                          ,[clm_service_dt]
                                          ,[clm_dt]
                                          ,[clm_payer_id]
                                          ,[clm_amt]
                                          ,[clm_paid_amt]
                                          ,[clm_status]
                                          ,[clm_payer_typ]
                                          ,[clm_deductible]
                                          ,[clm_coinsurance]
                                          ,[clm_copay]
                                          ,[clm_src_insert_dt]
                                          ,[clm_src_mod_id]
                                          ,[clm_insert_dt]
        )
        select 
         concat('HOSP2-',clm_id) as clm_id
        ,concat('HOSP2-',clm_trans_id) as clm_trans_id
        ,clm_pat_id
        ,concat('HOSP2-',clm_enctr_id) as clm_enctr_id
        ,concat('H2-',clm_provider_id) as clm_provider_id
        ,concat('HOSP2-',clm_dept_id) as clm_dept_id
        ,clm_service_dt
        ,clm_dt
        ,concat('HOSP2-',clm_payer_id) as clm_payer_id 
        ,clm_amt
        ,clm_paid_amt
        ,clm_status
        ,clm_payer_typ
        ,clm_deductible
        ,clm_coinsurance
        ,clm_copay
        ,clm_insert_dt
        ,clm_mod_id
        ,CAST(GETDATE() AS DATE)
        from 
        [HealthClaims].[bronze].claims_hosp_b
        
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
		PRINT 'Loading Providers Tables';
		PRINT '------------------------------------------------';
        -- Loading silver.[emr_hospital_a_providers]

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[emr_hospital_a_providers]';
		TRUNCATE TABLE silver.[emr_hospital_a_providers];
		PRINT '>> Inserting Data Into: silver.[emr_hospital_a_providers]';
		INSERT INTO silver.[emr_hospital_a_providers] (
                                                       [provider_id]
                                                      ,[provider_firstname]
                                                      ,[provider_lastname]
                                                      ,[provider_specialization]
                                                      ,[provider_dept_id]
                                                      ,[provider_npi]
                                                      ,[provider_insert_dt]
        )
        select 
           [provider_id]
          ,[provider_firstname]
          ,[provider_lastname]
          ,[provider_specialization]
          ,concat('HOSP1-',[provider_dept_id]) as [provider_dept_id]
          ,[provider_npi]
          ,CAST(GETDATE() AS DATE)
        from 
        [HealthClaims].[bronze].[emr_hospital_a_providers]
        
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.[emr_hospital_b_providers]

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[emr_hospital_b_providers]';
		TRUNCATE TABLE silver.emr_hospital_b_providers;
		PRINT '>> Inserting Data Into: silver.[emr_hospital_b_providers]';
		INSERT INTO silver.emr_hospital_b_providers (
                                                       [provider_id]
                                                      ,[provider_firstname]
                                                      ,[provider_lastname]
                                                      ,[provider_specialization]
                                                      ,[provider_dept_id]
                                                      ,[provider_npi]
                                                      ,[provider_insert_dt]
        )
        select 
           [provider_id]
          ,[provider_firstname]
          ,[provider_lastname]
          ,[provider_specialization]
          ,concat('HOSP2-',[provider_dept_id]) as [provider_dept_id]
          ,[provider_npi]
          ,CAST(GETDATE() AS DATE)
        from 
        [HealthClaims].[bronze].emr_hospital_b_providers
        
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
		PRINT 'Loading CPT Codes Tables';
		PRINT '------------------------------------------------';
        -- Loading silver.cpt_codes

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[cpt_codes]';
		TRUNCATE TABLE silver.[cpt_codes];
		PRINT '>> Inserting Data Into: silver.[cpt_codes]';
		INSERT INTO silver.[cpt_codes] (
                                                       [cpt_proc_cat]
                                                      ,[cpt_codes]
                                                      ,[cpt_proc_desc]
                                                      ,[cpt_code_status]
                                                      ,cpt_insert_dt
        )
        select 
           [cpt_proc_cat]
          ,[cpt_codes]
          ,[cpt_proc_desc]
          ,[cpt_code_status]
          ,CAST(GETDATE() AS DATE) as cpt_insert_dt
            from 
        [HealthClaims].[bronze].[cpt_codes];

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
		PRINT 'Loading Transactions Tables';
		PRINT '------------------------------------------------';
        -- Loading silver.emr_hospital_a_transaction

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[emr_hospital_a_transaction]';
		TRUNCATE TABLE silver.[emr_hospital_a_transaction];
		PRINT '>> Inserting Data Into: silver.[emr_hospital_a_transaction]';
		INSERT INTO silver.[emr_hospital_a_transaction] (
                                                        [trans_id]
                                                       ,[trans_enctr_id]
                                                       ,[trans_pat_id]
                                                       ,[trans_provider_id]
                                                       ,[trans_dept_id]
                                                       ,[trans_visit_dt]
                                                       ,[trans_service_dt]
                                                       ,[trans_paid_id]
                                                       ,[trans_visit_type]
                                                       ,[trans_amount]
                                                       ,[trans_amount_type]
                                                       ,[trans_paid_amount]
                                                       ,[trans_claim_id]
                                                       ,[trans_payer_id]
                                                       ,[trans_procedure_code]
                                                       ,[trans_icd_code]
                                                       ,[trans_lob]
                                                       ,[trans_medicaid_id]
                                                       ,[trans_medicare_id]
                                                       ,[trans_src_insert_dt]
                                                       ,[trans_src_modified_dt]
                                                       ,[trans_insert_dt]
        )
        select 
         concat('HOSP1-',[trans_id]) as [trans_id]
        ,concat('HOSP1-',[trans_enctr_id]) as [trans_enctr_id]
        ,[trans_pat_id]
        ,concat('H1-',[trans_provider_id]) as [trans_provider_id]
        ,concat('HOSP1-',[trans_dept_id]) as [trans_dept_id]
        ,[trans_visit_dt]
        ,[trans_service_dt]
        ,[trans_paid_id]
        ,[trans_visit_type]
        ,[trans_amount]
        ,[trans_amount_type]
        ,[trans_paid_amount]
        ,concat('HOSP1-',[trans_claim_id]) as [trans_claim_id]
        ,concat('HOSP1-',[trans_payer_id]) as [trans_payer_id]
        ,[trans_procedure_code]
        ,[trans_icd_code]
        ,[trans_lob]
        ,concat('HOSP1-',[trans_medicaid_id]) as [trans_medicaid_id]
        ,concat('HOSP1-',[trans_medicare_id]) as [trans_medicare_id]
        ,[trans_insert_dt]
        ,[trans_modified_dt]
        ,CAST(GETDATE() AS DATE) as [trans_insert_dt]
        from [HealthClaims].[bronze].[emr_hospital_a_transaction];

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.emr_hospital_b_transaction

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[emr_hospital_b_transaction]';
		TRUNCATE TABLE silver.emr_hospital_b_transaction;
		PRINT '>> Inserting Data Into: silver.[emr_hospital_b_transaction]';
		INSERT INTO silver.emr_hospital_b_transaction (
                                                        [trans_id]
                                                       ,[trans_enctr_id]
                                                       ,[trans_pat_id]
                                                       ,[trans_provider_id]
                                                       ,[trans_dept_id]
                                                       ,[trans_visit_dt]
                                                       ,[trans_service_dt]
                                                       ,[trans_paid_id]
                                                       ,[trans_visit_type]
                                                       ,[trans_amount]
                                                       ,[trans_amount_type]
                                                       ,[trans_paid_amount]
                                                       ,[trans_claim_id]
                                                       ,[trans_payer_id]
                                                       ,[trans_procedure_code]
                                                       ,[trans_icd_code]
                                                       ,[trans_lob]
                                                       ,[trans_medicaid_id]
                                                       ,[trans_medicare_id]
                                                       ,[trans_src_insert_dt]
                                                       ,[trans_src_modified_dt]
                                                       ,[trans_insert_dt]
        )
        select 
         concat('HOSP2-',[trans_id]) as [trans_id]
        ,concat('HOSP2-',[trans_enctr_id]) as [trans_enctr_id]
        ,[trans_pat_id]
        ,concat('H2-',[trans_provider_id]) as [trans_provider_id]
        ,concat('HOSP2-',[trans_dept_id]) as [trans_dept_id]
        ,[trans_visit_dt]
        ,[trans_service_dt]
        ,[trans_paid_id]
        ,[trans_visit_type]
        ,[trans_amount]
        ,[trans_amount_type]
        ,[trans_paid_amount]
        ,concat('HOSP2-',[trans_claim_id]) as [trans_claim_id]
        ,concat('HOSP2-',[trans_payer_id]) as [trans_payer_id]
        ,[trans_procedure_code]
        ,[trans_icd_code]
        ,[trans_lob]
        ,concat('HOSP2-',[trans_medicaid_id]) as [trans_medicaid_id]
        ,concat('HOSP2-',[trans_medicare_id]) as [trans_medicare_id]
        ,[trans_insert_dt]
        ,[trans_modified_dt]
        ,CAST(GETDATE() AS DATE) as [trans_insert_dt]
        from [HealthClaims].[bronze].emr_hospital_b_transaction;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
		PRINT 'Loading Encounters Tables';
		PRINT '------------------------------------------------';
        -- Loading silver.[emr_hospital_a_encounters]

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[emr_hospital_a_encounters]';
		TRUNCATE TABLE silver.emr_hospital_a_encounters;
		PRINT '>> Inserting Data Into: silver.[emr_hospital_a_encounters]';
		INSERT INTO silver.emr_hospital_a_encounters (
                                                        [enctr_id]
                                                       ,[enctr_pat_id]
                                                       ,[enctr_dt]
                                                       ,[enctr_type]
                                                       ,[enctr_provider_id]
                                                       ,[enctr_dept_id]
                                                       ,[enctr_procedure_code]
                                                       ,[enctr_src_insert_dt]
                                                       ,[enctr_src_modified_dt]
                                                       ,[enctr_insert_dt]
        )
       select 
         concat('HOSP1-',[enctr_id]) as [enctr_id]
        ,[enctr_pat_id]
        ,[enctr_dt]
        ,[enctr_type]
        ,concat('HOSP1-',[enctr_provider_id]) as [enctr_provider_id]
        ,concat('HOSP1-',[enctr_dept_id]) as [enctr_dept_id]
        ,[enctr_procedure_code]
        ,[enctr_insert_dt]
        ,[enctr_modified_dt]
        ,CAST(GETDATE() AS DATE) as [enctr_insert_dt]
        from [HealthClaims].[bronze].emr_hospital_a_encounters;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.[emr_hospital_b_encounters]

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[emr_hospital_b_encounters]';
		TRUNCATE TABLE silver.emr_hospital_b_encounters;
		PRINT '>> Inserting Data Into: silver.[emr_hospital_b_encounters]';
		INSERT INTO silver.emr_hospital_b_encounters (
                                                        [enctr_id]
                                                       ,[enctr_pat_id]
                                                       ,[enctr_dt]
                                                       ,[enctr_type]
                                                       ,[enctr_provider_id]
                                                       ,[enctr_dept_id]
                                                       ,[enctr_procedure_code]
                                                       ,[enctr_src_insert_dt]
                                                       ,[enctr_src_modified_dt]
                                                       ,[enctr_insert_dt]
        )
       select 
         concat('HOSP2-',[enctr_id]) as [enctr_id]
        ,[enctr_pat_id]
        ,[enctr_dt]
        ,[enctr_type]
        ,concat('HOSP2-',[enctr_provider_id]) as [enctr_provider_id]
        ,concat('HOSP2-',[enctr_dept_id]) as [enctr_dept_id]
        ,[enctr_procedure_code]
        ,[enctr_insert_dt]
        ,[enctr_modified_dt]
        ,CAST(GETDATE() AS DATE) as [enctr_insert_dt]
        from [HealthClaims].[bronze].emr_hospital_b_encounters;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
