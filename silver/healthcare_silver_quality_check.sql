----------------------check 1: unique patient id and -------------------------------
select count(*),pat_id from [HealthClaims].[bronze].[emr_hospital_a_patients]
group by pat_id having count(*)>1 or pat_id is null;
GO
-------------------check 2:length of ssn 
select count(*) from  [HealthClaims].[bronze].[emr_hospital_a_patients] where LEN(pat_ssn)<11;
GO
-------------------check 3 : invalid DOB
select count(*) from  [HealthClaims].[bronze].[emr_hospital_a_patients] where len(pat_dob)<10;
GO
select count(*) from  [HealthClaims].[bronze].[emr_hospital_a_patients] where (pat_dob)>CAST(GETDATE() AS DATE);
GO
--------------------------------check4: modified date
select count(*) from  [HealthClaims].[bronze].[emr_hospital_a_patients] where (pat_modified_dt)<=pat_dob;
GO
--------------------------------check 5 : invalid phone number 
select * from  [HealthClaims].[bronze].[emr_hospital_a_patients] where substring(pat_phonenumber,1,1)='-' or CHARINDEX('x', pat_phonenumber)>0;
GO
---------------------------Check for hospital B
-------------------check 1: unique patient id and -------------------------------
select count(*),pat_id from [HealthClaims].[bronze].[emr_hospital_b_patients]
group by pat_id having count(*)>1 or pat_id is null;
GO
-------------------check 2:length of ssn 
select count(*) from  [HealthClaims].[bronze].[emr_hospital_b_patients] where LEN(pat_ssn)<11;
GO
-------------------check 3 : invalid DOB
select count(*) from  [HealthClaims].[bronze].[emr_hospital_b_patients] where len(pat_dob)<10;
GO
select count(*) from  [HealthClaims].[bronze].[emr_hospital_b_patients] where (pat_dob)>CAST(GETDATE() AS DATE);
GO
--------------------check4: modified date
select count(*) from  [HealthClaims].[bronze].[emr_hospital_b_patients] where (pat_modified_dt)<=pat_dob;
GO

--------------------Validate checks-------------------------------------
----------------------check 1: unique patient id and -------------------------------
select count(*),pat_id from [HealthClaims].[silver].[emr_hospital_a_patients]
group by pat_id having count(*)>1 or pat_id is null;
GO
-------------------check 2:length of ssn 
select count(*) from  [HealthClaims].[silver].[emr_hospital_a_patients] where LEN(pat_ssn)<11;
GO
-------------------check 3 : invalid DOB
select count(*) from  [HealthClaims].[silver].[emr_hospital_a_patients] where len(pat_dob)<10;
GO
select count(*) from  [HealthClaims].[silver].[emr_hospital_a_patients] where (pat_dob)>CAST(GETDATE() AS DATE);
GO
--------------------------------check4: modified date
select count(*) from  [HealthClaims].[silver].[emr_hospital_a_patients] where (pat_src_modified_dt)<pat_dob;
GO

----------------------validate table B
----------------------check 1: unique patient id  -------------------------------
select count(*),pat_id from [HealthClaims].[silver].[emr_hospital_b_patients]
group by pat_id having count(*)>1 or pat_id is null;
GO
-------------------check 2:length of ssn 
select count(*) from  [HealthClaims].[silver].[emr_hospital_b_patients] where LEN(pat_ssn)<11;
GO
-------------------check 3 : invalid DOB
select count(*) from  [HealthClaims].[silver].[emr_hospital_b_patients] where len(pat_dob)<10;
GO
select count(*) from  [HealthClaims].[silver].[emr_hospital_b_patients] where (pat_dob)>CAST(GETDATE() AS DATE);
GO
--------------------------------check4: modified date
select count(*) from  [HealthClaims].[silver].[emr_hospital_b_patients] where (pat_modified_dt)<pat_dob;
GO

----------------------------------quality check for depatments table 
SELECT TOP (1000) [dept_id]
      ,[dept_name]
  FROM [HealthClaims].[bronze].[emr_hospital_a_departments];

SELECT TOP (1000) [dept_id]
      ,[dept_name]
  FROM [HealthClaims].[bronze].[emr_hospital_b_departments];

--------------quality check for claims A table 
------check 1 :- unique claim id
select count(*),clm_id from [HealthClaims].bronze.claims_hosp_a
group by clm_id having count(*)>1 or clm_id is null;
GO
-------check 2 :- check payer type
select distinct(clm_payer_id) from [HealthClaims].bronze.claims_hosp_a;
GO
--------check 3:- check claim status
select distinct(clm_status) from [HealthClaims].bronze.claims_hosp_a;
GO
--------check 4:- check claim payer type
select distinct(clm_payer_typ) from [HealthClaims].bronze.claims_hosp_a;
GO
--------check 5:- check claim detuctible 
select count(*) from [HealthClaims].bronze.claims_hosp_a where clm_deductible<0;
GO
--------check 6 :- check claim coinsurance 
select count(*) from [HealthClaims].bronze.claims_hosp_a where clm_coinsurance<0;
GO
--------check 7 :- check claim copay 
select count(*) from [HealthClaims].bronze.claims_hosp_a where clm_copay<0;
GO
--------------quality check for claims B table 
------check 1 :- unique claim id
select count(*),clm_id from [HealthClaims].bronze.claims_hosp_b
group by clm_id having count(*)>1 or clm_id is null;
GO
-------check 2 :- check payer type
select distinct(clm_payer_id) from [HealthClaims].bronze.claims_hosp_b;
GO
--------check 3:- check claim status
select distinct(clm_status) from [HealthClaims].bronze.claims_hosp_b;
GO
--------check 4:- check claim payer type
select distinct(clm_payer_typ) from [HealthClaims].bronze.claims_hosp_b;
GO
--------check 5:- check claim detuctible 
select count(*) from [HealthClaims].bronze.claims_hosp_b where clm_deductible<0;
GO
--------check 6 :- check claim coinsurance 
select count(*) from [HealthClaims].bronze.claims_hosp_b where clm_coinsurance<0;
GO
--------check 7 :- check claim copay 
select count(*) from [HealthClaims].bronze.claims_hosp_b where clm_copay<0;
GO
--------------quality check for provider A table 
-------Check 1 :- duplicate entries and null primary key
select count(*),[provider_id] from [HealthClaims].[bronze].[emr_hospital_a_providers]
group by [provider_id] having  [provider_id] is null or count(*) >1;
GO
------check 2:- check provider specialization
select distinct [provider_specialization] from [HealthClaims].[bronze].[emr_hospital_a_providers];
GO
-------check3 :- length of npi
select distinct(len([provider_npi])) from [HealthClaims].[bronze].[emr_hospital_a_providers]
;
GO
--------------quality check for provider B table 
-------Check 1 :- duplicate entries and null primary key
select count(*),[provider_id] from [HealthClaims].[bronze].[emr_hospital_b_providers]
group by [provider_id] having  [provider_id] is null or count(*) >1;
GO
------check 2:- check provider specialization
select distinct [provider_specialization] from [HealthClaims].[bronze].[emr_hospital_b_providers];
GO
-------check3 :- length of npi
select distinct(len([provider_npi])) from [HealthClaims].[bronze].[emr_hospital_b_providers]
;
GO
--------------quality check for CPT codes table 
---------------check 1: check for cpt procedure codes
select distinct([cpt_proc_cat]) from [HealthClaims].[bronze].[cpt_codes];
GO
---------------check 2: check for cpt codes status
select distinct([cpt_code_status]) from [HealthClaims].[bronze].[cpt_codes];
GO
---------------check 3: check for cpt codes status
select distinct([cpt_codes]) from [HealthClaims].[bronze].[cpt_codes];
GO

