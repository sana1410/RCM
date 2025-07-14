/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
USE HealthClaims;
GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading HOSPITAL A Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_a_patients';
		TRUNCATE TABLE bronze.emr_hospital_a_patients;
		PRINT '>> Inserting Data Into: bronze.emr_hospital_a_patients';
		BULK INSERT bronze.emr_hospital_a_patients
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-a\patients.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_a_departments';
		TRUNCATE TABLE bronze.emr_hospital_a_departments;
		PRINT '>> Inserting Data Into: bronze.emr_hospital_a_departments';
		BULK INSERT bronze.emr_hospital_a_departments
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-a\departments.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_a_encounters';
		TRUNCATE TABLE bronze.emr_hospital_a_encounters;
		PRINT '>> Inserting Data Into: bronze.emr_hospital_a_encounters';
		BULK INSERT bronze.emr_hospital_a_encounters
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-a\encounters.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_a_providers';
		TRUNCATE TABLE bronze.emr_hospital_a_providers;
		PRINT '>> Inserting Data Into: bronze.emr_hospital_a_providers';
		BULK INSERT bronze.emr_hospital_a_providers
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-a\providers.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_a_transaction';
		TRUNCATE TABLE bronze.emr_hospital_a_transaction;
		PRINT '>> Inserting Data Into: bronze.emr_hospital_a_transaction';
		BULK INSERT bronze.emr_hospital_a_transaction
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-a\transactions.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading HOSPITAL B Tables';
		PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_b_patients';
		TRUNCATE TABLE bronze.emr_hospital_b_patients;

		PRINT '>> Inserting Data Into: bronze.emr_hospital_b_patients';
		BULK INSERT bronze.emr_hospital_b_patients
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-b\patients.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_b_departments';
		TRUNCATE TABLE bronze.emr_hospital_b_departments;

		PRINT '>> Inserting Data Into: bronze.emr_hospital_b_departments';
		BULK INSERT bronze.emr_hospital_b_departments
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-b\departments.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_b_encounters';
		TRUNCATE TABLE bronze.emr_hospital_b_encounters;

		PRINT '>> Inserting Data Into: bronze.emr_hospital_b_encounters';
		BULK INSERT bronze.emr_hospital_b_encounters
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-b\encounters.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_b_providers';
		TRUNCATE TABLE bronze.emr_hospital_b_providers;

		PRINT '>> Inserting Data Into: bronze.emr_hospital_b_providers';
		BULK INSERT bronze.emr_hospital_b_providers
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-b\providers.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.emr_hospital_b_transaction';
		TRUNCATE TABLE bronze.emr_hospital_b_transaction;

		PRINT '>> Inserting Data Into: bronze.emr_hospital_b_transaction';
		BULK INSERT bronze.emr_hospital_b_transaction
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-b\transactions.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading CPT CODES Table';
		PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.cpt_codes';
		TRUNCATE TABLE bronze.cpt_codes;
		PRINT '>> Inserting Data Into: bronze.cpt_codes';
		BULK INSERT bronze.cpt_codes
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\cptcodes\cptcodes.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading CLaims Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.claims_hosp_a';
		TRUNCATE TABLE bronze.claims_hosp_a;
		PRINT '>> Inserting Data Into: bronze.claims_hosp_a';
		BULK INSERT bronze.claims_hosp_a
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\claims\hospital1_claim_data.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.claims_hosp_b';
		TRUNCATE TABLE bronze.claims_hosp_b;
		PRINT '>> Inserting Data Into: bronze.claims_hosp_b';
		BULK INSERT bronze.claims_hosp_b
		FROM 'C:\Users\sanay\Documents\forage\Trendytech-Azure-Project\datasets\claims\hospital2_claim_data.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
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
