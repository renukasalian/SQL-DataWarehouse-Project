/*
=======================================================================================
Stored Procedure : Load Bronze Layer (source → bronze)
=======================================================================================
Purpose:
    Loads data into the Bronze layer from external CSV files.

Description:
    - Truncates each Bronze table to ensure a clean load (avoids duplicates).
    - Performs BULK INSERT operations from source CSV files.
    - Prints the row count, load duration for each table and total load duration.
*/


--crm_cust_info TABLE

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================================';

		PRINT '------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------';

		PRINT '>>> Truncating Table : bronze.crm_prd_info';
	
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\renu\data_warehouse_project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		SELECT COUNT(*) FROM bronze.crm_cust_info;
		SET @end_time = GETDATE()

		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------'

		--prd_cust_info TABLE

	
		PRINT '>>> Truncating Table : bronze.crm_prd_cust_info';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_prd_cust_info

		BULK INSERT bronze.crm_prd_cust_info
		FROM 'D:\renu\data_warehouse_project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		SELECT COUNT(*) FROM bronze.crm_prd_cust_info;
		SET @end_time = GETDATE()
		 
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------'

		--crm_sales_details TABLE

	
		PRINT '>>> Truncating Table : bronze.crm_sales_details';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\renu\data_warehouse_project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		SELECT COUNT(*) FROM bronze.crm_sales_details;
		SET @end_time = GETDATE()

		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------'
	
		--erp_cust_az12 TABLE

		PRINT '------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------';

		PRINT '>>> Truncating Table : bronze.erp_cust_az12';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_cust_az12

		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\renu\data_warehouse_project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		SELECT COUNT(*) FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE()
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------'

		--erp_loc_a101 TABLE

	
		PRINT '>>> Truncating Table : bronze.erp_loc_a101';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_loc_a101

		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\renu\data_warehouse_project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		SELECT COUNT(*) FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE()

		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------'

		--erp_pz_cat_g1v2 TABLE

		PRINT '>>> Truncating Table : bronze.erp_pz_cat_g1v2';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\renu\data_warehouse_project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE()
		SET @batch_end_time = GETDATE()
		PRINT '>>Load Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------';
		
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



EXEC bronze.load_bronze

