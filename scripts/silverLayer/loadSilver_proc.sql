/*
=============================================================================================================
Stored Procedure : Load Silver Layer (Bronze -> Silver)
=============================================================================================================
Script Purpose:
  This stored procedure performs ETL (Extract, Transform, Load) process.
  It truncates each Silver table, if table exists.
  Applies all data cleaning and transformation rules, reloads clean, standardized data into the Silver layer.
  Inserts the cleaned data from bronze layer into silver layer.
  Additionally it also prints the overal and table load duration.
*/



--creating stored procedure


--creating stored procedure

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		PRINT '==========================================';
		PRINT 'Loading Silver Layer';
		PRINT '==========================================';

		PRINT '------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------';

		--Loading silver.crm_cust_info
		PRINT '>>Truncating TABLE : silver.crm_cust_info';
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		PRINT '>>Truncating TABLE : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>Inserting Data Into: silver.crm_cust_info';
		
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>Inserting Data Into: silver.crm_cust_info';
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		INSERT INTO silver.crm_prd_cust_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_')AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		cast(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_cust_info;
		SET @end_time = GETDATE()
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '>>>-----------';





		

		--Loading silver.crm_prd_cust_info
		PRINT '>>Truncating TABLE : silver.crm_prd_cust_info';
		TRUNCATE TABLE silver.crm_prd_cust_info;
		PRINT '>>Inserting Data Into: silver.crm_prd_cust_info';
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		INSERT INTO silver.crm_prd_cust_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_')AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		cast(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_cust_info;


		--Loading silver.crm_sales_details
		PRINT '>>Truncating TABLE : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>Inserting Data Into: silver.crm_sales_details';
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_quantity,
		sls_sales,
		sls_price
		)

		SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt<0 
					OR LEN(sls_order_dt)!=8 
					OR sls_order_dt > 20500909
					OR sls_order_dt < 19000101 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END sls_order_dt,
		CASE WHEN sls_ship_dt < 0 
					OR LEN(sls_ship_dt)!=8 
					OR sls_ship_dt > 20500909
					OR sls_ship_dt < 19000101 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,
		sls_due_dt,
		sls_quantity,
		CASE WHEN sls_sales IS NULL 
					OR sls_sales <= 0 
					OR sls_sales != sls_quantity*ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		CASE WHEN sls_price < = 0 OR sls_price IS NULL 
					THEN sls_sales/ NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_Details
		SET @end_time = GETDATE()
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '>>>-----------';





		--Loading silver.erp_cust_az12
		PRINT '>>Truncating TABLE : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>Inserting Data Into: silver.erp_cust_az12';
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE()
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '>>>-----------';



		--Loading silver.erp_loc_a101
		PRINT '>>Truncating TABLE : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>Inserting Data Into: silver.erp_loc_a101';
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		INSERT 
		INTO silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT 
		REPLACE(cid,'-','') cid,
		CASE  
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE()
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '>>>-----------';


		--Loading silver.erp_px_cat_g1v2
		PRINT '>>Truncating TABLE : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>Inserting Data Into: silver.erp_px_cat_g1v2';
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		INSERT INTO silver.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)
		SELECT 
		id, cat, subcat, maintenance
		FROM bronze.erp_px_cat_g1v2 ;
		SET @end_time = GETDATE()
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '>>>-----------';

		PRINT '=====================================';
		PRINT 'TOTAL DURATION : ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR) + ' seconds';
		PRINT '=====================================';

END TRY
BEGIN CATCH
		PRINT '=================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '=================================================';
	END CATCH

END


EXEC silver.load_silver
