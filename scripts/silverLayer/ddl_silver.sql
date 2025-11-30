/*
=============================================================================================================
DDL : CREATE SILVER LAYER
=============================================================================================================
Script Purpose:
  This script initializes all tables under the 'silver' schema.
  Any existing tables will be removed first to ensure a clean rebuild.
Execute this script whenever you need to re-define structure of the bronze layer DDL.
*/

-- Before creating table checking if it already exists. 
USE DataWarehouse;

IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(40),
	cst_firstname VARCHAR(40),
	cst_lastname VARCHAR(40),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO
IF OBJECT_ID ('silver.crm_prd_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_cust_info;
CREATE TABLE silver.crm_prd_cust_info(
	prd_id INT,
	cat_id VARCHAR(50), --THIS ROW ADDED WHILE TRANSFORMING bronze.crm_prd_cust_info TABLE
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(40),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);

GO
IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
	sls_ord_num VARCHAR(30),
	sls_prd_key VARCHAR(30),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO
IF OBJECT_ID ('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
	cid VARCHAR(40),
	bdate DATE,
	gen VARCHAR(15),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO
IF OBJECT_ID ('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
	id VARCHAR(20),
	cat VARCHAR(30),
	subcat VARCHAR(30),
	maintenance VARCHAR(15),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO
IF OBJECT_ID ('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
	cid VARCHAR(30),
	cntry VARCHAR(20),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);



