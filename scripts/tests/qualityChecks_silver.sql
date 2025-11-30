
/*
===========================================================================================================================
QUALITY CHECKS : SILVER LAYER
===========================================================================================================================
Script Purpose:
  This script performs various quality checks for data accuracy, data inconsistency, standardizations,
  data enchrichment 
  across 'silver' schema.
  the quality checks includes:
  -checking NULL and duplicated primary keys.
  -checking for unwanted spaces.
  -checking data standardization and consistency
  -invalid data ranges and orders.
  -Following business rules for related fields.
*/


**************************************************************************************************************************
  Checking 'silver.crm_cust_info'
**************************************************************************************************************************
--checking for NUlls and duplicates in primary key.
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL;

--checking the duplicated id
SELECT * FROM silver.crm_cust_info WHERE cst_id=29466;

--check for unwanted spaces
PRINT '>>entries with unwanted spaces';
SELECT cst_firstname,cst_lastname,cst_gndr FROM silver.crm_cust_info 
WHERE cst_firstname ! = TRIM(cst_firstname) OR cst_lastname ! = TRIM(cst_lastname) OR cst_gndr != TRIM(cst_gndr);

--checking data standardization and consistency
SELECT distinct cst_gndr FROM silver.crm_cust_info;
SELECT distinct cst_marital_status FROM silver.crm_cust_info;


**************************************************************************************************************************
  Checking 'silver.crm_prd_cust_info'
**************************************************************************************************************************

--checking duplicate and nulls for primary key
SELECT prd_id, COUNT(*) FROM silver.crm_prd_cust_info
 GROUP BY prd_id HAVING COUNT(*)>1 OR prd_id IS NULL;


 --check for unwanted spaces
 SELECT * FROM silver.crm_prd_cust_info WHERE cat_id!= TRIM(cat_id) OR prd_key!=TRIM(prd_key) 
			OR prd_nm !=TRIM(prd_nm) OR prd_line != TRIM(prd_line); 

--CHECK FOR NULLS IN PRD_COST
SELECT prd_cost FROM silver.crm_prd_cust_info WHERE prd_cost<1 OR prd_cost IS NULL;

--DATA STANDARDIZATION AND CONSISTENCY
SELECT DISTINCT prd_line FROM silver.crm_prd_cust_info 

--CHECK FOR INVALID DATA ORDERS
SELECT * FROM silver.crm_prd_cust_info WHERE prd_start_dt>prd_end_dt;

--Final look of silver.crm_prd_cust_info TABLE
SELECT * FROM silver.crm_prd_cust_info;


**************************************************************************************************************************
  Checking 'silver.crm_sales_details'
**************************************************************************************************************************
--checking business rule {sales = quantity * price and sales!= 0,negative or null}
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details 
WHERE sls_sales != sls_quantity*sls_price 
		OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales,sls_quantity,sls_price

--final look into the table
SELECT * FROM silver.crm_sales_details;


**************************************************************************************************************************
  Checking 'silver.erp_cust_az12'
**************************************************************************************************************************
--CHECKING DATA QUALITY OF silver.erp_cust_az12 table
--CHECK DATATYPE OF BDATE
SELECT bdate FROM silver.erp_cust_az12;

--check if customers exits whose age >100 OR customers born in future 
SELECT DISTINCT bdate FROM silver.erp_cust_az12 WHERE bdate > '1920-01-01' OR bdate > GETDATE();

--check for data standardization and consistencyt
SELECT DISTINCT gen FROM silver.erp_cust_az12;

--final look into the table
SELECT *FROM silver.erp_cust_az12;

**************************************************************************************************************************
  Checking 'silver.erp_cust_az12'
**************************************************************************************************************************
--check for data quality in silver.erp_cust_a101 table

SELECT DISTINCT cntry FROM
silver.erp_loc_a101
ORDER BY cntry;

--final look into table
SELECT * FROM silver.erp_loc_a101;


**************************************************************************************************************************
  Checking 'silver.erp_cust_az12'
**************************************************************************************************************************
--SINCE THE DATA WAS ALREADY IN CLEANED FORMAT IN BRONZE LAYER CHECKING IN SILVER IS NOT REQUIRED.


